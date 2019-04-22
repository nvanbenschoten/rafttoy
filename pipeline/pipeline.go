package pipeline

import (
	"sync"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/transport"
	"go.etcd.io/etcd/raft"
)

type Pipeline interface {
	Init(int32, *raft.RawNode, storage.Storage, transport.Transport, *proposal.Tracker)
	Start()
	Stop()
	RunOnce(sync.Locker)
}
