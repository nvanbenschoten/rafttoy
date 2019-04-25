package main

import (
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nvanbenschoten/raft-toy/metric"
	"github.com/nvanbenschoten/raft-toy/peer"
	"github.com/nvanbenschoten/raft-toy/pipeline"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/storage/engine"
	"github.com/nvanbenschoten/raft-toy/transport"
)

func newPeer(epoch int32) *peer.Peer {
	cfg := cfgFromFlags()
	cfg.Epoch = epoch

	//    CONFIGURE PLUGGABLE COMPONENTS HERE:
	// It would be nice to inject these directly from
	// the benchmarks, but that doesn't work well when
	// coordinating across processes. This is easiest.

	// Storage.
	//  WAL.
	// w := wal.NewMem()
	// w := engine.NewPebble(*dataDir).(wal.Wal)
	//  Engine.
	// e := engine.NewMem()
	// e := engine.NewPebble(*dataDir)
	//  Combined.
	// s := storage.CombineWalAndEngine(w, e)
	s := engine.NewPebble(*dataDir).(storage.Storage)

	// Transport.
	t := transport.NewGRPC()

	// Pipeline.
	pl := pipeline.NewBasic()
	// pl := pipeline.NewAsyncApplier(false /* earlyAck */)
	// pl := pipeline.NewAsyncApplier(true /* earlyAck */)

	return peer.New(cfg, s, t, pl)
}

func main() {

	printMetrics := metric.Enable(*recordMetrics)
	defer printMetrics()

	p := newPeer(0)

	// Make sure we clean up before exiting.
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		p.Stop()
		printMetrics()
		os.Exit(0)
	}()

	// If we're not running load, we enter follower mode.
	if !*runLoad {
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
