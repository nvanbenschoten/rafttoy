package engine

import "go.etcd.io/etcd/raft/raftpb"

type Engine interface {
	SetHardState(raftpb.HardState)
	ApplyEntry(raftpb.Entry)
	Clear()
	Close()
}
