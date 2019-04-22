package peer

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvanbenschoten/raft-toy/pipeline"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/transport"
	"github.com/nvanbenschoten/raft-toy/util"
	"go.etcd.io/etcd/raft"
)

type Peer struct {
	mu   sync.Mutex
	sig  sync.Cond
	done int32

	n  *raft.RawNode
	s  storage.Storage
	t  transport.Transport
	pl pipeline.Pipeline

	pi int64
	pt proposal.Tracker
}

// New creates a new Peer.
func New(
	cfg *raft.Config,
	peers []raft.Peer,
	s storage.Storage,
	t transport.Transport,
	pl pipeline.Pipeline,
) *Peer {
	cfg.Storage = util.NewRaftStorage(s)
	n, err := raft.NewRawNode(cfg, peers)
	if err != nil {
		log.Fatal(err)
	}

	p := &Peer{
		n:  n,
		s:  s,
		t:  t,
		pl: pl,
		pt: proposal.MakeTracker(),
	}
	p.pl.Init(n, s, t, &p.pt)
	p.sig.L = &p.mu
	return p
}

func (p *Peer) Run() {
	p.pl.Start()
	go p.ticker()

	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		for !p.n.HasReady() {
			if p.stopped() {
				return
			}
			p.sig.Wait()
		}
		p.pl.RunOnce(&p.mu)
	}
}

func (p *Peer) ticker() {
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for !p.stopped() {
		<-t.C
		p.mu.Lock()
		p.n.Tick()
		p.mu.Unlock()
		p.sig.Signal()
	}
}

func (p *Peer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt32(&p.done, 1)
	p.pt.FinishAll()
	p.sig.Signal()
	p.pl.Stop()
}

func (p *Peer) stopped() bool {
	return atomic.LoadInt32(&p.done) == 1
}

func (p *Peer) Propose(prop proposal.Proposal) bool {
	c := make(chan struct{})
	enc := proposal.Encode(prop)

	p.mu.Lock()
	if p.stopped() {
		p.mu.Unlock()
		return false
	}
	p.pi++
	enc.SetID(p.pi)
	err := p.n.Propose(enc)
	if err != nil {
		p.mu.Unlock()
		return false
	}
	p.pt.Register(enc, c)
	p.mu.Unlock()
	p.sig.Signal()

	<-c
	return true
}
