package storage

import (
	"log"

	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type mem struct {
	m *raft.MemoryStorage
}

// NewMem creates a new in-memory storage engine.
func NewMem() Storage {
	return &mem{
		m: raft.NewMemoryStorage(),
	}
}

func (m *mem) SetHardState(st raftpb.HardState) {
	if err := m.m.SetHardState(st); err != nil {
		log.Fatal(err)
	}
}
