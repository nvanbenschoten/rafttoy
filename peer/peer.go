package peer

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvanbenschoten/raft-toy/pipeline"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/transport"
	"github.com/nvanbenschoten/raft-toy/util"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Peer is a member of a Raft consensus group.
type Peer struct {
	mu     sync.RWMutex
	sig    sync.Cond // signaled to wake-up Raft loop
	revSig sync.Cond // signaled to wake-up waiting proposers
	done   int32

	cfg Config
	n   *raft.RawNode
	s   storage.Storage
	t   transport.Transport
	pl  pipeline.Pipeline

	pi int64
	pb propBuf
	pt proposal.Tracker
}

// Config contains configurations for constructing a Peer.
type Config struct {
	Epoch     int32
	ID        uint64
	Peers     []raft.Peer
	SelfAddr  string
	PeerAddrs map[uint64]string
}

func makeRaftCfg(cfg Config, s storage.Storage) *raft.Config {
	return &raft.Config{
		ID:                        cfg.ID,
		ElectionTick:              3,
		HeartbeatTick:             1,
		MaxSizePerMsg:             math.MaxUint64,
		MaxInflightMsgs:           int(math.MaxInt64),
		Storage:                   util.NewRaftStorage(s),
		PreVote:                   true,
		DisableProposalForwarding: true,
	}
}

// New creates a new Peer.
func New(
	cfg Config,
	s storage.Storage,
	t transport.Transport,
	pl pipeline.Pipeline,
) *Peer {
	raftCfg := makeRaftCfg(cfg, s)
	n, err := raft.NewRawNode(raftCfg, cfg.Peers)
	if err != nil {
		log.Fatal(err)
	}

	p := &Peer{
		cfg: cfg,
		n:   n,
		s:   s,
		t:   t,
		pl:  pl,
		pt:  proposal.MakeTracker(),
	}
	p.t.Init(cfg.SelfAddr, cfg.PeerAddrs)
	p.pl.Init(p.cfg.Epoch, p.n, p.s, p.t, &p.pt)
	go p.t.Serve(p)
	p.sig.L = &p.mu
	p.revSig.L = p.mu.RLocker()
	return p
}

// Run starts the Peer's processing loop.
func (p *Peer) Run() {
	p.pl.Start()
	go p.ticker()

	p.mu.Lock()
	for {
		for p.pb.lenWLocked() == 0 && !p.n.HasReady() {
			if p.stopped() {
				p.mu.Unlock()
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

// Stop stops all processing and releases all resources held by Peer.
func (p *Peer) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt32(&p.done, 1)
	p.pt.FinishAll()
	p.sig.Signal()
	p.Close()
}

func (p *Peer) stopped() bool {
	return atomic.LoadInt32(&p.done) == 1
}

// Close releases resources held by Peer. It does not acquire
// any locks, so it can be used during an unclean shutdown.
func (p *Peer) Close() {
	p.s.Close()
	p.t.Close()
	p.pl.Stop()
}

// Campaign causes the Peer to transition to the candidate state
// and attempt to acquire Raft leadership.
func (p *Peer) Campaign() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n.Campaign()
	p.sig.Signal()
}

// Propose proposes the provided update to the Raft state machine.
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

// HandleMessage implements transport.RaftHandler.
func (p *Peer) HandleMessage(epoch int32, msg *raftpb.Message) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if epoch < p.cfg.Epoch {
		return
	}
	if epoch > p.cfg.Epoch {
		log.Printf("bumping test epoch to %d", epoch)
		p.bumpEpochLocked(epoch)
	}
	p.n.Step(*msg)
	p.sig.Signal()
}

func (p *Peer) bumpEpochLocked(epoch int32) {
	if p.pb.lenWLocked() > 0 || p.pt.Len() > 0 {
		log.Fatal("cannot reset peer with in-flight proposals")
	}
	// Clear all persistent state and create a new Raft node.
	p.cfg.Epoch = epoch
	p.s.Truncate()
	p.s.Clear()
	raftCfg := makeRaftCfg(p.cfg, p.s)
	n, err := raft.NewRawNode(raftCfg, p.cfg.Peers)
	if err != nil {
		log.Fatal(err)
	}
	p.n = n
	p.pl.Init(p.cfg.Epoch, p.n, p.s, p.t, &p.pt)
}
