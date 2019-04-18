package wal

import (
	"log"

	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type mem struct {
	m *raft.MemoryStorage
}

// NewMem creates a new in-memory write-ahead log.
func NewMem() Wal {
	return &mem{
		m: raft.NewMemoryStorage(),
	}
}

func (m *mem) Append(ents []raftpb.Entry) {
	if err := m.m.Append(ents); err != nil {
		log.Fatal(err)
	}
}

func (m *mem) Entries(lo, hi uint64) []raftpb.Entry {
	ents, err := m.m.Entries(lo, hi, 0)
	if err != nil {
		log.Fatal(err)
	}
	return ents
}

func (m *mem) Term(i uint64) uint64 {
	t, err := m.m.Term(i)
	if err != nil {
		log.Fatal(err)
	}
	return t
}

func (m *mem) LastIndex() uint64 {
	i, err := m.m.LastIndex()
	if err != nil {
		log.Fatal(err)
	}
	return i
}

func (m *mem) FirstIndex() uint64 {
	i, err := m.m.FirstIndex()
	if err != nil {
		log.Fatal(err)
	}
	return i
}
