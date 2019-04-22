package transport

import (
	"go.etcd.io/etcd/raft/raftpb"
)

type Transport interface {
	Init(string, map[uint64]string)
	Serve(RaftHandler)
	Send(int32, []raftpb.Message)
	Close()
}

type RaftHandler interface {
	HandleMessage(int32, *raftpb.Message)
}
