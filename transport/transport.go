package transport

import (
	"github.com/nvanbenschoten/rafttoy/config"
	transpb "github.com/nvanbenschoten/rafttoy/transport/transportpb"
	"go.etcd.io/etcd/raft/raftpb"
)

// Transport handles RPC messages for Raft coordination.
type Transport interface {
	Init(addr string, peers map[uint64]string)
	Serve(RaftHandler)
	Send(epoch config.TestEpoch, msgs []raftpb.Message)
	Close()
}

// RaftHandler is an object capable of accepting incoming Raft messages.
type RaftHandler interface {
	HandleMessage(*transpb.RaftMsg)
}
