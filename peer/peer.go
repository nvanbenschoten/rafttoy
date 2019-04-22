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
	mu     sync.RWMutex
	sig    sync.Cond
	sigAt  int32
	revSig sync.Cond
	done   int32

	n  *raft.RawNode
	s  storage.Storage
	t  transport.Transport
	pl pipeline.Pipeline

	pi int64
	pb propBuf
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
	p.revSig.L = p.mu.RLocker()
	return p
}

func (p *Peer) Run() {
	p.pl.Start()
	go p.ticker()

	p.mu.Lock()
	defer p.mu.Unlock()
	for {
		for p.pb.lenWLocked() == 0 && !p.n.HasReady() {
			if p.stopped() {
				return
			}
			p.sig.Wait()
		}
		p.flushPropsLocked()
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
	prop.ID = atomic.AddInt64(&p.pi, 1)
	enc := proposal.Encode(prop)
	c := make(chan bool, 1)
	el := propBufElem{enc, c}

	p.mu.RLock()
	for !p.pb.addRLocked(el) {
		p.revSig.Wait()
	}
	p.mu.RUnlock()

	p.sig.Signal()
	if p.stopped() {
		return false
	}
	return <-c
}

func (p *Peer) flushPropsLocked() {
	b := p.pb.flushWLocked()
	for _, e := range b {
		err := p.n.Propose(e.enc)
		if err != nil {
			e.c <- false
		} else {
			p.pt.Register(e.enc, e.c)
		}
	}
	if len(b) == propBufCap {
		p.revSig.Broadcast()
	}
}
