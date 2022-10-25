package wal

import (
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"

	"go.etcd.io/etcd/raft/v3/raftpb"
	etcdwal "go.etcd.io/etcd/server/v3/wal"
	"go.uber.org/zap"
)

const dirPrefix = "etcd-wal"

type etcdWal struct {
	w   *etcdwal.WAL
	dir string
	c   LogCache
}

// NewEtcdWal creates a new write-ahead log using the etcd/wal package.
func NewEtcdWal(root string) Wal {
	dir := randDir(root)
	w, err := etcdwal.Create(zap.NewNop(), dir, nil)
	if err != nil {
		log.Fatal(err)
	}
	return &etcdWal{
		w:   w,
		dir: dir,
		// We can't allow the cache to perform compactions
		// of old indices because we can't read from the WAL
		// file directly.
		c: MakeLogCache(false),
	}
}

func randDir(root string) string {
	return filepath.Join(root, dirPrefix, strconv.FormatUint(rand.Uint64(), 10))
}

func (w *etcdWal) Append(ents []raftpb.Entry, st raftpb.HardState, sync bool) {
	if err := w.w.Save(st, ents); err != nil {
		log.Fatal(err)
	}
	w.c.UpdateOnAppend(ents)
}

func (w *etcdWal) Entries(lo, hi uint64) []raftpb.Entry {
	n := hi - lo
	ents := make([]raftpb.Entry, 0, n)
	ents, _ = w.c.Entries(ents, lo, hi)
	if uint64(len(ents)) != hi-lo {
		log.Fatalf("missing entries in entry cache [%d,%d)", lo, hi)
	}
	return ents
}

func (w *etcdWal) Term(i uint64) uint64 {
	if t, ok := w.c.Term(i); ok {
		return t
	}
	return 0
}

func (w *etcdWal) LastIndex() uint64 {
	return w.c.LastIndex()
}

func (w *etcdWal) FirstIndex() uint64 {
	return w.c.FirstIndex()
}

func (w *etcdWal) Truncate() {
	w.CloseWal()
	var err error
	if w.w, err = etcdwal.Create(zap.NewNop(), w.dir, nil); err != nil {
		log.Fatal(err)
	}
	w.c.Reset()
}

func (w *etcdWal) CloseWal() {
	if err := w.w.Close(); err != nil {
		log.Fatal(err)
	}
	if err := os.RemoveAll(w.dir); err != nil {
		log.Fatal(err)
	}
}
