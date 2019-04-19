package transport

import "go.etcd.io/etcd/raft/raftpb"

type Transport interface {
	Serve(func(*raftpb.Message))
	Send([]raftpb.Message)
	Close()
}
