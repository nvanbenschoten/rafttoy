package peer

import (
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvanbenschoten/rafttoy/config"
	"github.com/nvanbenschoten/rafttoy/pipeline"
	"github.com/nvanbenschoten/rafttoy/proposal"
	"github.com/nvanbenschoten/rafttoy/storage"
	"github.com/nvanbenschoten/rafttoy/transport"
	transpb "github.com/nvanbenschoten/rafttoy/transport/transportpb"
	"github.com/nvanbenschoten/rafttoy/util"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Peer is a member of a Raft consensus group. Its primary roles are to:
// 1. route incoming Raft messages
// 2. periodically tick the Raft RawNode
// 3. serve as a scheduler for Raft proposal pipeline events
type Peer struct {
	mu   sync.Mutex
	sig  chan struct{} // signaled to wake-up Raft loop
	done atomic.Bool
	wg   sync.WaitGroup

	cfg Config
	n   *raft.RawNode
	s   storage.Storage
	t   transport.Transport
	pl  pipeline.Pipeline

	pi int64
	pb propBuf
	pt proposal.Tracker

	leader       atomic.Bool
	msgs         chan *transpb.RaftMsg
	flushPropsFn func([]propBufElem)
}

// Config contains configurations for constructing a Peer.
type Config struct {
	Epoch     config.TestEpoch
	ID        uint64
	Peers     []raft.Peer
	SelfAddr  string
	PeerAddrs map[uint64]string
}

func makeRaftCfg(cfg Config, s storage.Storage, asyncStorageWrites bool) *raft.Config {
	return &raft.Config{
		ID:                        cfg.ID,
		ElectionTick:              50,
		HeartbeatTick:             5,
		MaxSizePerMsg:             math.MaxUint64,
		MaxInflightMsgs:           math.MaxInt,
		Storage:                   util.NewRaftStorage(s),
		AsyncStorageWrites:        asyncStorageWrites,
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
	raftCfg := makeRaftCfg(cfg, s, pl.AsyncStorageWrites())
	n, err := raft.NewRawNode(raftCfg)
	if err != nil {
		log.Fatal(err)
	}
	n.Bootstrap(cfg.Peers)

	p := &Peer{
		sig:  make(chan struct{}, 1),
		cfg:  cfg,
		n:    n,
		s:    s,
		t:    t,
		pl:   pl,
		pt:   proposal.MakeTracker(),
		msgs: make(chan *transpb.RaftMsg, 1024),
	}
	p.t.Init(cfg.SelfAddr, cfg.PeerAddrs)
	p.pl.Init(p.cfg.Epoch, &p.mu, p.n, p.s, p.t, &p.pt, p.signal)
	p.pb.init()
	p.flushPropsFn = p.flushProps
	go p.t.Serve(p)
	return p
}

// Run starts the Peer's processing loop.
func (p *Peer) Run() {
	p.wg.Add(2)
	p.pl.Start()
	go p.ticker()
	defer p.wg.Done()

	for {
		<-p.sig
		if p.stopped() {
			p.mu.Lock()
			p.pb.flush(p.flushPropsFn)
			p.mu.Unlock()
			return
		}
		p.mu.Lock()
		p.flushMsgs()
		p.pb.flush(p.flushPropsFn)
		p.pl.RunOnce()
		p.mu.Unlock()
	}
}

func (p *Peer) signal() {
	select {
	case p.sig <- struct{}{}:
	default:
		// Already signaled.
	}
}

func (p *Peer) ticker() {
	defer p.wg.Done()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for !p.stopped() {
		<-t.C
		p.mu.Lock()
		if p.leader.Load() {
			if p.n.BasicStatus().RaftState != raft.StateLeader {
				log.Fatal("raft leadership unexpectedly lost")
			}
		}
		p.n.Tick()
		p.mu.Unlock()
		p.signal()
	}
}

// Stop stops all processing and releases all resources held by Peer.
func (p *Peer) Stop() {
	p.done.Store(true)
	p.signal()
	p.t.Close()
	p.wg.Wait()
	p.pt.FinishAll()
	p.pl.Stop()
	p.s.CloseEngine()
	p.s.CloseWal()
}

func (p *Peer) stopped() bool {
	return p.done.Load()
}

// Campaign causes the Peer to transition to the candidate state
// and attempt to acquire Raft leadership.
func (p *Peer) Campaign() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.n.Campaign()
	p.signal()
}

// Propose proposes the provided update to the Raft state machine.
func (p *Peer) Propose(prop proposal.Proposal) bool {
	return p.ProposeWith(proposal.Encode(prop), make(chan bool, 1))
}

// ProposeWith proposes the provided encoded update to the Raft
// state machine. Channel c is expected to have a capacity of 1.
func (p *Peer) ProposeWith(enc proposal.EncProposal, c chan bool) bool {
	enc.SetID(atomic.AddInt64(&p.pi, 1))
	el := propBufElem{enc, c}

	p.pb.add(el)
	p.signal()
	if p.stopped() {
		return false
	}
	return <-c
}

func (p *Peer) flushProps(es []propBufElem) {
	ents := make([]raftpb.Entry, len(es))
	for i := range es {
		ents[i].Data = es[i].enc
		p.pt.Register(es[i].enc, es[i].c)
	}
	if err := p.n.Step(raftpb.Message{
		Type:    raftpb.MsgProp,
		From:    p.cfg.ID,
		Entries: ents,
	}); err != nil {
		for i := range es {
			p.pt.Finish(es[i].enc.GetID(), false)
		}
	}
}

// HandleMessage implements transport.RaftHandler.
func (p *Peer) HandleMessage(m *transpb.RaftMsg) {
	p.msgs <- m
	p.signal()
}

func (p *Peer) flushMsgs() {
	for {
		select {
		case m := <-p.msgs:
			if m.Epoch.Less(p.cfg.Epoch) {
				return
			}
			if p.cfg.Epoch.Less(m.Epoch) {
				log.Printf("bumping test epoch to %s", m.Epoch)
				p.bumpEpoch(m.Epoch)
			}
			for i := range m.Msgs {
				p.n.Step(m.Msgs[i])
			}
		default:
			return
		}
	}
}

func (p *Peer) bumpEpoch(epoch config.TestEpoch) {
	// Clear all persistent state and create a new Raft node.
	p.pl.Pause()
	p.s.Truncate()
	p.s.Clear()
	p.cfg.Epoch = epoch
	raftCfg := makeRaftCfg(p.cfg, p.s, p.pl.AsyncStorageWrites())
	n, err := raft.NewRawNode(raftCfg)
	if err != nil {
		log.Fatal(err)
	}
	n.Bootstrap(p.cfg.Peers)
	p.n = n
	p.pl.Resume(epoch, n)
}

// BecomeLeader waits for the peer to become the Raft leader.
func (p *Peer) BecomeLeader() {
	var lastCamp time.Time
	for {
		time.Sleep(10 * time.Millisecond)
		p.mu.Lock()
		st := p.n.Status()
		p.mu.Unlock()

		// Wait until we're the raft leader.
		if st.RaftState != raft.StateLeader {
			if now := time.Now(); now.Sub(lastCamp) > 250*time.Millisecond {
				// Campaign every 250ms.
				p.Campaign()
				lastCamp = now
			}
			continue
		}

		// Wait for all peers to catch up to the same log index.
		caughtUp := true
		var match uint64
		for _, pr := range st.Progress {
			if match == 0 {
				match = pr.Match
			} else {
				caughtUp = caughtUp && match == pr.Match
			}
		}
		if !caughtUp {
			continue
		}

		// Submit a proposal to verify that we're actually the leader.
		prop := proposal.Proposal{
			Key: []byte("key"),
			Val: make([]byte, 1),
		}
		if !p.Propose(prop) {
			continue
		}

		// Success.
		break
	}
	p.leader.Store(true)
}
