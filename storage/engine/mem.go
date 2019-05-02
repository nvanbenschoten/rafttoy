package engine

import (
	"log"
	"unsafe"

	"github.com/nvanbenschoten/rafttoy/proposal"
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

func (m *mem) SetHardState(st raftpb.HardState, _ bool) {
	if err := m.m.SetHardState(st); err != nil {
		log.Fatal(err)
	}
}

func (m *mem) ApplyEntry(ent raftpb.Entry) {
	prop := proposal.Decode(ent.Data)
	m.kv[unsafeString(prop.Key)] = prop.Val
}

func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func (m *mem) Clear() {
	m.m = raft.NewMemoryStorage()
	m.kv = make(map[string][]byte)
}

func (m *mem) CloseEngine() {
	// No-op.
}
