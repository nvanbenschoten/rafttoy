package storage

import "go.etcd.io/etcd/raft/raftpb"

type Storage interface {
	SetHardState(raftpb.HardState)
}
