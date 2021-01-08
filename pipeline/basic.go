package pipeline

import (
	"sync"

	"github.com/nvanbenschoten/rafttoy/config"
	"github.com/nvanbenschoten/rafttoy/proposal"
	"github.com/nvanbenschoten/rafttoy/storage"
	"github.com/nvanbenschoten/rafttoy/transport"
	"go.etcd.io/etcd/raft/v3"
)

// basic is a standard proposal pipeline. It mirrors the
// approach described in the etcd/raft documentation.
type basic struct {
	epoch  config.TestEpoch
	l      sync.Locker
	n      *raft.RawNode
	s      storage.Storage
	t      transport.Transport
	pt     *proposal.Tracker
	signal func()
}

// NewBasic creates a new "basic" pipeline.
func NewBasic() Pipeline {
	return new(basic)
}

func (pl *basic) Init(
	epoch config.TestEpoch,
	l sync.Locker,
	n *raft.RawNode,
	s storage.Storage,
	t transport.Transport,
	pt *proposal.Tracker,
	signal func(),
) {
	pl.epoch = epoch
	pl.l = l
	pl.n = n
	pl.s = s
	pl.t = t
	pl.pt = pt
	pl.signal = signal
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
func (pl *basic) Resume(epoch config.TestEpoch, n *raft.RawNode) {
	pl.epoch = epoch
	pl.n = n
}
func (pl *basic) Stop() {}
