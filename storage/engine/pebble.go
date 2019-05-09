package engine

import (
	"bytes"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/nvanbenschoten/rafttoy/proposal"
	"github.com/nvanbenschoten/rafttoy/storage/wal"
	pdb "github.com/petermattis/pebble"
	"github.com/petermattis/pebble/db"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const dirPrefix = "pebble-data"

var minMetaKey = []byte{0x00}
var maxMetaKey = []byte{0x01}

// MinDataKey is the minimum key allowable data key.
var MinDataKey = maxMetaKey
var maxDataKey = []byte{0x02}
var minLogKey = maxDataKey
var maxLogKey = []byte{0x03}
var hardStateKey = append(minMetaKey, []byte("hs")...)

type pebble struct {
	db   *pdb.DB
	opts *db.Options
	dir  string
	c    wal.LogCache
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
		c:    wal.MakeLogCache(false),
	}
}

func randDir(root string) string {
	return filepath.Join(root, dirPrefix, strconv.FormatUint(rand.Uint64(), 10))
}

func (p *pebble) SetHardState(st raftpb.HardState, sync bool) {
	buf, err := st.Marshal()
	if err != nil {
		log.Fatal(err)
	}
	if err := p.db.Set(hardStateKey, buf, optsForSync(sync)); err != nil {
		log.Fatal(err)
	}
}

func (p *pebble) ApplyEntry(ent raftpb.Entry) {
	prop := proposal.Decode(ent.Data)
	if err := p.db.Set(prop.Key, prop.Val, db.NoSync); err != nil {
		log.Fatal(err)
	}
}

// pebble implements engine.BatchingEngine.
func (p *pebble) ApplyEntries(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	b := p.db.NewBatch()
	defer b.Close()
	for i := range ents {
		d := ents[i].Data
		if len(d) == 0 {
			continue
		}
		prop := proposal.Decode(d)
		if err := b.Set(prop.Key, prop.Val, nil); err != nil {
			log.Fatal(err)
		}
	}
	if err := b.Commit(db.NoSync); err != nil {
		log.Fatal(err)
	}
}

func (p *pebble) Clear() {
	p.CloseWal()
	db, err := pdb.Open(p.dir, p.opts)
	if err != nil {
		log.Fatal(err)
	}
	p.db = db
}

func (p *pebble) CloseEngine() {
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

func (p *pebble) Append(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	b := p.db.NewBatch()
	defer b.Close()
	appendEntsToBatch(b, ents)
	if err := b.Commit(db.Sync); err != nil {
		log.Fatal(err)
	}
	p.c.UpdateOnAppend(ents)
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
	ents := make([]raftpb.Entry, 0, n)
	ents, hitIndex := p.c.Entries(ents, lo, hi)
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
	if t, ok := p.c.Term(i); ok {
		return t
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
	return p.c.LastIndex()
}

func (p *pebble) FirstIndex() uint64 {
	return p.c.FirstIndex()
}

func (p *pebble) Truncate() {
	p.Clear()
	p.c.Reset()
}

func (p *pebble) CloseWal() {
	p.CloseEngine()
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
	defer b.Close()
	if len(ents) > 0 {
		appendEntsToBatch(b, ents)
		p.c.UpdateOnAppend(ents)
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
	if err := b.Commit(optsForSync(sync)); err != nil {
		log.Fatal(err)
	}
}

func optsForSync(sync bool) *db.WriteOptions {
	if sync {
		return db.Sync
	}
	return db.NoSync
}
