package engine

import "go.etcd.io/etcd/raft/raftpb"

// Engine represents a storage engine that Raft entries are applied to.
type Engine interface {
	SetHardState(raftpb.HardState)
	ApplyEntry(raftpb.Entry)
	Clear()
	Close()
}
