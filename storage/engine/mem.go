package engine

import (
	"log"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type mem struct {
	m  *raft.MemoryStorage
	kv map[string][]byte
}

// NewMem creates a new in-memory storage engine.
func NewMem() Engine {
	return &mem{
		m:  raft.NewMemoryStorage(),
		kv: make(map[string][]byte),
	}
}

func (m *mem) SetHardState(st raftpb.HardState) {
	if err := m.m.SetHardState(st); err != nil {
		log.Fatal(err)
	}
}

func (m *mem) ApplyEntry(ent raftpb.Entry) {
	prop := proposal.Decode(ent.Data)
	m.kv[string(prop.Key)] = prop.Val
}
