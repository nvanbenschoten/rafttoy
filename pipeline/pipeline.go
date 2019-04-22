package pipeline

import (
	"sync"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/transport"
	"go.etcd.io/etcd/raft"
)

type Pipeline interface {
	Init(n *raft.RawNode, s storage.Storage, t transport.Transport, pt *proposal.Tracker)
	Start()
	Stop()
	RunOnce(sync.Locker)
}
