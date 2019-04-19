package main

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nvanbenschoten/raft-toy/pipeline"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/util"
	"github.com/nvanbenschoten/raft-toy/wal"
	"go.etcd.io/etcd/raft"
)

type peer struct {
	mu   sync.Mutex
	sig  sync.Cond
	done int32

	n  *raft.RawNode
	pl pipeline.Pipeline

	pi int64
	pt proposal.Tracker
}

func newPeer() *peer {
	w := wal.NewMem()
	s := storage.NewMem()
	rs := util.NewRaftStorage(w, s)

	c := &raft.Config{
		ID:              0x01,
		ElectionTick:    3,
		HeartbeatTick:   1,
		Storage:         rs,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	peers := []raft.Peer{{ID: 0x01}, {ID: 0x02}, {ID: 0x03}}
	n, err := raft.NewRawNode(c, peers)
	if err != nil {
		log.Fatal(err)
	}

	p := new(peer)
	p.sig.L = &p.mu
	p.n = n
	p.pl = pipeline.NewBasic(n, w, s, nil, &p.pt)
	p.pt = proposal.MakeTracker()
	return p
}

func (p *peer) run() {
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
		p.pl.RunOnce()
	}
}

func (p *peer) ticker() {
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

func (p *peer) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	atomic.StoreInt32(&p.done, 1)
	p.pt.FinishAll()
	p.sig.Signal()
	p.pl.Stop()
}

func (p *peer) stopped() bool {
	return atomic.LoadInt32(&p.done) == 1
}

func (p *peer) propose(prop proposal.Proposal) bool {
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

func main() {
	p := newPeer()
	go p.run()

	prop := proposal.Proposal{
		Key: []byte("key"),
		Val: []byte("val"),
	}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !p.stopped() {
				if p.propose(prop) {
					log.Print("successful proposal")
				} else {
					log.Print("unsuccessful proposal")
				}
				time.Sleep(1 * time.Second)
			}
		}()
	}
	time.Sleep(10 * time.Second)
	p.stop()
	wg.Wait()
}
