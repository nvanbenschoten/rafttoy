package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/nvanbenschoten/rafttoy/metric"
	"github.com/nvanbenschoten/rafttoy/peer"
	"github.com/nvanbenschoten/rafttoy/pipeline"
	"github.com/nvanbenschoten/rafttoy/proposal"
	"github.com/nvanbenschoten/rafttoy/storage"
	"github.com/nvanbenschoten/rafttoy/storage/engine"
	"github.com/nvanbenschoten/rafttoy/transport"
)

func newPeer(cfg peer.Config) *peer.Peer {
	//    CONFIGURE PLUGGABLE COMPONENTS HERE:
	// It would be nice to inject these directly from
	// the benchmarks, but that doesn't work well when
	// coordinating across processes. This is easiest.

	// Storage.
	//  WAL.
	// w := wal.NewMem()
	// w := engine.NewPebble(*dataDir, false).(wal.Wal)
	// w := wal.NewEtcdWal(*dataDir)
	//  Engine.
	// e := engine.NewMem()
	// e := engine.NewPebble(*dataDir, false)
	//  Combined.
	// s := storage.CombineWalAndEngine(w, e)
	s := engine.NewPebble(*dataDir, false).(storage.Storage)

	// Transport.
	t := transport.NewGRPC()

	// Pipeline.
	var pl pipeline.Pipeline
	switch *pipelineImpl {
	case "basic":
		pl = pipeline.NewBasic()
	case "parallel-append":
		pl = pipeline.NewParallelAppender()
	case "async-apply":
		pl = pipeline.NewAsyncApplier(false /* earlyAck */, false /* lazyFollower */)
	case "async-apply-early-ack":
		pl = pipeline.NewAsyncApplier(true /* earlyAck */, false /* lazyFollower */)
	case "async-apply-early-ack-lazy-follower":
		pl = pipeline.NewAsyncApplier(true /* earlyAck */, true /* lazyFollower */)
	default:
		log.Fatalf("unknown pipeline %q", *pipelineImpl)
	}

	return peer.New(cfg, s, t, pl)
}

func main() {
	servePProf(*pprof)
	printMetrics := metric.Enable(*recordMetrics)
	defer printMetrics()

	cfg := cfgFromFlags()
	p := newPeer(cfg)

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

func servePProf(port int) {
	if port < 0 {
		return
	}
	addr := fmt.Sprintf(":%d", port)
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("serving pprof on %s", ln.Addr())
	go func() {
		if err := http.Serve(ln, nil); err != nil {
			log.Fatal(err)
		}
	}()
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
	p.WaitForAllCaughtUp()
}
