package wal

import "go.etcd.io/etcd/raft/raftpb"

// Wal represents a write-ahead log to store Raft log entries.
type Wal interface {
	Append([]raftpb.Entry)
	Truncate()
	Entries(lo, hi uint64) []raftpb.Entry
	Term(i uint64) uint64
	LastIndex() uint64
	FirstIndex() uint64
}
