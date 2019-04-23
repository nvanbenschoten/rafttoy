package engine

import (
	"bytes"
	"encoding/binary"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"github.com/nvanbenschoten/raft-toy/proposal"
	pdb "github.com/petermattis/pebble"
	"github.com/petermattis/pebble/db"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

const dirPrefix = "pebble-data"

var minDataKey = []byte{0x00}
var maxDataKey = []byte{0x01}
var hardStateKey = append(minDataKey, []byte("hard-state")...)

type pebble struct {
	db  *pdb.DB
	dir string
}

// NewPebble creates an LSM-based storage engine using Pebble.
func NewPebble() Engine {
	dir := randDir()
	db, err := pdb.Open(dir, &db.Options{})
	if err != nil {
		log.Fatal(err)
	}
	return &pebble{db: db, dir: dir}
}

func randDir() string {
	return filepath.Join(dirPrefix, strconv.FormatUint(rand.Uint64(), 10))
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
	n := NewPebble().(*pebble)
	*p = *n
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

var minLogKey = []byte{0x01}
var maxLogKey = []byte{0x02}

const maxLogKeyLen = 1 + binary.MaxVarintLen64

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
	appendEntsToBatch(b, ents)
	b.Commit(db.Sync)
}

func appendEntsToBatch(b *pdb.Batch, ents []raftpb.Entry) {
	for i := range ents {
		ent := &ents[i]

		var kArr [maxLogKeyLen]byte
		k := kArr[:encodeRaftLogKey(kArr[:], ent.Index)]

		buf, err := ent.Marshal()
		if err != nil {
			log.Fatal(err)
		}
		if err := b.Set(k, buf, nil); err != nil {
			log.Fatal(err)
		}
	}
}

func (p *pebble) Entries(lo, hi uint64) []raftpb.Entry {
	var kLoArr, kHiArr [maxLogKeyLen]byte
	kLo := kLoArr[:encodeRaftLogKey(kLoArr[:], lo)]
	kHi := kHiArr[:encodeRaftLogKey(kHiArr[:], hi)]

	var ents []raftpb.Entry
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
	it := p.db.NewIter(nil)
	defer it.Close()
	it.SeekLT(maxLogKey)
	for !it.Valid() || bytes.Compare(it.Key(), minLogKey) < 0 {
		// See raft.MemoryStorage.LastIndex.
		return 0
	}
	i := decodeRaftLogKey(it.Key())
	if err := it.Close(); err != nil {
		log.Fatal(err)
	}
	return i
}

func (p *pebble) FirstIndex() uint64 {
	it := p.db.NewIter(nil)
	defer it.Close()
	it.SeekGE(minLogKey)
	for !it.Valid() || bytes.Compare(it.Key(), maxLogKey) > 0 {
		// See raft.MemoryStorage.FirstIndex.
		return 1
	}
	i := decodeRaftLogKey(it.Key())
	if err := it.Close(); err != nil {
		log.Fatal(err)
	}
	return i
}

func (p *pebble) Truncate() {
	p.Close()
	n := NewPebble().(*pebble)
	*p = *n
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
