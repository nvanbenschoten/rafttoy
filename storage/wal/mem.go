package wal

import (
	"log"
	"math"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

type mem struct {
	m     *raft.MemoryStorage
	delay time.Duration
}

// NewMem creates a new in-memory write-ahead log.
func NewMem(delay time.Duration) Wal {
	return &mem{
		m:     raft.NewMemoryStorage(),
		delay: delay,
	}
}

func (m *mem) Append(ents []raftpb.Entry) {
	if err := m.m.Append(ents); err != nil {
		log.Fatal(err)
	}
	if m.delay != 0 {
		time.Sleep(m.delay)
	}
}

func (m *mem) Entries(lo, hi uint64) []raftpb.Entry {
	ents, err := m.m.Entries(lo, hi, math.MaxUint64)
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

func (m *mem) Truncate() {
	m.m = raft.NewMemoryStorage()
}

func (m *mem) CloseWal() {
	// No-op.
}
