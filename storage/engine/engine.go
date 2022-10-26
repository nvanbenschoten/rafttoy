package engine

import "go.etcd.io/etcd/raft/v3/raftpb"

// Engine represents a storage engine that Raft entries are applied to.
type Engine interface {
	ApplyEntry(raftpb.Entry)
	Clear()
	CloseEngine()
}

// BatchingEngine represents a storage engine that can apply a batch of
// Raft entries all at once. This is often more efficient than applying
// an entry at a time.
type BatchingEngine interface {
	Engine
	ApplyEntries([]raftpb.Entry)
}
