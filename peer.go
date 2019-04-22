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
	"go.etcd.io/etcd/raft"
)

func newPeer() *peer.Peer {
	// Raft config.
	cfg := &raft.Config{
		ID:              0x01,
		ElectionTick:    3,
		HeartbeatTick:   1,
		MaxSizePerMsg:   4096,
		MaxInflightMsgs: 256,
	}
	peers := []raft.Peer{{ID: 0x01}}

	// Storage.
	w := wal.NewMem()
	e := engine.NewMem()
	s := storage.CombineWalAndEngine(w, e)

	// Transport.
	var t transport.Transport

	// Pipeline.
	pl := pipeline.NewBasic()

	return peer.New(cfg, peers, s, t, pl)
}

func main() {
	p := newPeer()
	go p.Run()
	defer p.Stop()

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
