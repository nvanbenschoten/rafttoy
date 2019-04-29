package pipeline

import (
	"sync"

	"github.com/nvanbenschoten/rafttoy/proposal"
	"github.com/nvanbenschoten/rafttoy/storage"
	"github.com/nvanbenschoten/rafttoy/transport"
	"go.etcd.io/etcd/raft"
)

// basic is a standard proposal pipeline. It mirrors the
// approach described in the etcd/raft documentation.
type basic struct {
	epoch int32
	l     sync.Locker
	n     *raft.RawNode
	s     storage.Storage
	t     transport.Transport
	pt    *proposal.Tracker
}

// NewBasic creates a new "basic" pipeline.
func NewBasic() Pipeline {
	return new(basic)
}

func (pl *basic) Init(
	epoch int32,
	l sync.Locker,
	n *raft.RawNode,
	s storage.Storage,
	t transport.Transport,
	pt *proposal.Tracker,
) {
	pl.epoch = epoch
	pl.l = l
	pl.n = n
	pl.s = s
	pl.t = t
	pl.pt = pt
}

func (pl *basic) RunOnce() {
	defer measurePipelineLat()()
	rd := pl.n.Ready()
	pl.l.Unlock()
	saveToDisk(pl.s, rd.Entries, rd.HardState, rd.MustSync)
	sendMessages(pl.t, pl.epoch, rd.Messages)
	processSnapshot(rd.Snapshot)
	applyToStore(pl.n, pl.s, pl.pt, pl.l, rd.CommittedEntries, true)
	pl.l.Lock()
	pl.n.Advance(rd)
}

func (pl *basic) Start() {}
func (pl *basic) Pause() {}
func (pl *basic) Resume(epoch int32, n *raft.RawNode) {
	pl.epoch = epoch
	pl.n = n
}
func (pl *basic) Stop() {}
