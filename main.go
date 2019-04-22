package main

import (
	"log"
	"sync"
	"time"

	"github.com/nvanbenschoten/raft-toy/peer"
	"github.com/nvanbenschoten/raft-toy/pipeline"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/storage/engine"
	"github.com/nvanbenschoten/raft-toy/storage/wal"
	"github.com/nvanbenschoten/raft-toy/transport"
)

func newPeer(epoch int32) *peer.Peer {
	cfg := parseFlags()

	// Storage.
	w := wal.NewMem()
	e := engine.NewMem()
	s := storage.CombineWalAndEngine(w, e)

	// Transport.
	t := transport.NewGRPC()

	// Pipeline.
	pl := pipeline.NewBasic()

	return peer.New(epoch, cfg, s, t, pl)
}

func main() {
	p := newPeer(0)
	if !runLoad {
		p.Run()
		return
	}

	go p.Run()
	defer p.Stop()

	// Wait for the initial leader election to complete.
	becomeLeader(p)

	prop := proposal.Proposal{
		Key: []byte("key"),
		Val: []byte("val"),
	}
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				if p.Propose(prop) {
					log.Print("successful proposal")
				} else {
					log.Print("unsuccessful proposal")
				}
				time.Sleep(1 * time.Second)
			}
		}()
	}
}

func becomeLeader(p *peer.Peer) {
	prop := proposal.Proposal{
		Key: []byte("key"),
		Val: make([]byte, 1),
	}
	var lastCamp time.Time
	for !p.Propose(prop) {
		if now := time.Now(); now.Sub(lastCamp) > 250*time.Millisecond {
			p.Campaign()
			lastCamp = now
		}
		time.Sleep(1 * time.Millisecond)
	}
}
