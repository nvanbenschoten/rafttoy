package pipeline

import (
	"sync"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/transport"
	"go.etcd.io/etcd/raft"
)

// Pipeline represents an implementation of a Raft proposal pipeline. It manages
// the interactions between a Raft "raw node" and the various components that
// the Raft "raw node" needs to coordinate with.
type Pipeline interface {
	Init(int32, *raft.RawNode, storage.Storage, transport.Transport, *proposal.Tracker)
	Start()
	Stop()
	RunOnce(sync.Locker)
}
