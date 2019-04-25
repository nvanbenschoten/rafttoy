package engine

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/util/raftentry"
	pdb "github.com/petermattis/pebble"
	"github.com/petermattis/pebble/db"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const dirPrefix = "pebble-data"

var minMetaKey = []byte{0x00}
var maxMetaKey = []byte{0x01}
var minDataKey = maxMetaKey
var maxDataKey = []byte{0x02}
var minLogKey = maxDataKey
var maxLogKey = []byte{0x03}
var hardStateKey = append(minMetaKey, []byte("hs")...)

type pebble struct {
	db   *pdb.DB
	opts *db.Options
	dir  string
	c    logCache
}

// NewPebble creates an LSM-based storage engine using Pebble.
func NewPebble(root string, disableWAL bool) Engine {
	if disableWAL {
		log.Fatal("disable wal is currently broken")
	}
	dir := randDir(root)
	opts := &db.Options{
		DisableWAL: disableWAL,
	}
	db, err := pdb.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	return &pebble{
		db:   db,
		opts: opts,
		dir:  dir,
		c:    makeLogCache(),
	}
}

func randDir(root string) string {
	return filepath.Join(root, dirPrefix, strconv.FormatUint(rand.Uint64(), 10))
}

func (p *pebble) SetHardState(st raftpb.HardState) {
	buf, err := st.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	if err := p.db.Set(hardStateKey, buf, db.Sync); err != nil {
		log.Fatal(err)
	}
}

func (p *pebble) ApplyEntry(ent raftpb.Entry) {
	prop := proposal.Decode(ent.Data)
	if err := p.db.Set(prop.Key, prop.Val, db.NoSync); err != nil {
		log.Fatal(err)
	}
}

func (p *pebble) Clear() {
	p.Close()
	db, err := pdb.Open(p.dir, p.opts)
	if err != nil {
		log.Fatal(err)
	}
	p.db = db
}

func (p *pebble) Close() {
	if err := p.db.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.RemoveAll(p.dir); err != nil {
		log.Fatal(err)
	}
}

//////////////////////////////////////////
// A Pebble database can also be used for
// the Raft log by implementing wal.Wal.
//////////////////////////////////////////

const maxLogKeyLen = 10

func encodeRaftLogKey(k []byte, v uint64) int {
	k = append(k[:0], minLogKey...)
	switch {
	case v <= 0xff:
		k = append(k, 1, byte(v))
	case v <= 0xffff:
		k = append(k, 2, byte(v>>8), byte(v))
	case v <= 0xffffff:
		k = append(k, 3, byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffff:
		k = append(k, 4, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	case v <= 0xffffffffff:
		k = append(k, 5, byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8),
			byte(v))
	case v <= 0xffffffffffff:
		k = append(k, 6, byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16),
			byte(v>>8), byte(v))
	case v <= 0xffffffffffffff:
		k = append(k, 7, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24),
			byte(v>>16), byte(v>>8), byte(v))
	default:
		k = append(k, 8, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
			byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
	return len(k)
}

func decodeRaftLogKey(k []byte) uint64 {
	k = k[len(minLogKey):] // skip prefix
	length := int(k[0])
	k = k[1:] // skip length
	var v uint64
	for _, t := range k[:length] {
		v = (v << 8) | uint64(t)
	}
	return v
}

// logCache remembers a few facts about the Raft log.
type logCache struct {
	lastIndex uint64
	lastTerm  uint64
	ec        *raftentry.Cache
}

func makeLogCache() logCache {
	return logCache{lastIndex: 0, ec: raftentry.NewCache(4 << 20)}
}

func (c *logCache) updateForEnts(ents []raftpb.Entry) {
	last := ents[len(ents)-1]
	c.lastIndex = last.Index
	c.lastTerm = last.Term
	c.ec.Add(0, ents, true)
}

func (p *pebble) Append(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	b := p.db.NewBatch()
	appendEntsToBatch(b, ents)
	b.Commit(db.Sync)
	p.c.updateForEnts(ents)
}

func appendEntsToBatch(b *pdb.Batch, ents []raftpb.Entry) {
	var buf []byte
	for i := range ents {
		ent := &ents[i]

		var kArr [maxLogKeyLen]byte
		k := kArr[:encodeRaftLogKey(kArr[:], ent.Index)]

		s := ent.Size()
		if cap(buf) < s {
			buf = make([]byte, s)
		} else {
			buf = buf[:s]
		}
		if _, err := ent.MarshalTo(buf); err != nil {
			log.Fatal(err)
		}
		if err := b.Set(k, buf, nil); err != nil {
			log.Fatal(err)
		}
	}
}

func (p *pebble) Entries(lo, hi uint64) []raftpb.Entry {
	n := hi - lo
	if n > 100 {
		n = 100
	}
	ents := make([]raftpb.Entry, 0, n)
	ents, _, hitIndex, _ := p.c.ec.Scan(ents, 0, lo, hi, math.MaxUint64)
	if uint64(len(ents)) == hi-lo {
		return ents
	}

	var kLoArr, kHiArr [maxLogKeyLen]byte
	kLo := kLoArr[:encodeRaftLogKey(kLoArr[:], hitIndex)]
	kHi := kHiArr[:encodeRaftLogKey(kHiArr[:], hi)]

	it := p.db.NewIter(nil)
	for valid := it.SeekGE(kLo); valid && bytes.Compare(it.Key(), kHi) < 0; valid = it.Next() {
		ents = append(ents, raftpb.Entry{})
		if err := ents[len(ents)-1].Unmarshal(it.Value()); err != nil {
			log.Fatal(err)
		}
	}
	if err := it.Close(); err != nil {
		log.Fatal(err)
	}
	return ents
}

func (p *pebble) Term(i uint64) uint64 {
	if p.c.lastIndex == i && p.c.lastTerm != 0 {
		return p.c.lastTerm
	}
	if e, ok := p.c.ec.Get(0, i); ok {
		return e.Term
	}

	var kArr [maxLogKeyLen]byte
	k := kArr[:encodeRaftLogKey(kArr[:], i)]

	buf, err := p.db.Get(k)
	if err != nil {
		if err == db.ErrNotFound {
			return 0
		}
		log.Fatal(err)
	}

	var ent raftpb.Entry
	if err := ent.Unmarshal(buf); err != nil {
		log.Fatal(err)
	}
	return ent.Term
}

func (p *pebble) LastIndex() uint64 {
	return p.c.lastIndex
}

func (p *pebble) FirstIndex() uint64 {
	return 1
}

func (p *pebble) Truncate() {
	p.Clear()
	p.c = makeLogCache()
}

//////////////////////////////////////////////////////
// When used as a Raft log and as an Engine, a Pebble
// database can implement AtomicStorage.
//////////////////////////////////////////////////////

func (p *pebble) AppendAndSetHardState(ents []raftpb.Entry, st raftpb.HardState, sync bool) {
	if len(ents) == 0 && raft.IsEmptyHardState(st) {
		return
	}
	b := p.db.NewBatch()
	if len(ents) > 0 {
		appendEntsToBatch(b, ents)
		p.c.updateForEnts(ents)
	}
	if !raft.IsEmptyHardState(st) {
		buf, err := st.Marshal()
		if err != nil {
			log.Fatal(err)
		}
		if err := b.Set(hardStateKey, buf, nil); err != nil {
			log.Fatal(err)
		}
	}
	opts := db.NoSync
	if sync {
		opts = db.Sync
	}
	b.Commit(opts)
}
