package main

import (
	"errors"
	"testing"
	"time"

	"github.com/nvanbenschoten/rafttoy/config"
	"github.com/nvanbenschoten/rafttoy/metric"
	"github.com/nvanbenschoten/rafttoy/storage/engine"
	"github.com/nvanbenschoten/rafttoy/util"
	"github.com/nvanbenschoten/rafttoy/workload"
	"golang.org/x/sync/errgroup"
)

// processNanos represents the time that the process started.
var processNanos = time.Now().UnixNano()

// benchIter represents a single iteration of a `go bench` loop. Tracking it and
// threading it through Raft messages allows a single instance of `go bench` to
// coordinate Raft state machine resets across a cluster of Go processes.
var benchIter int32

func newEpoch() config.TestEpoch {
	benchIter++
	return config.TestEpoch{
		ProcessNanos: processNanos,
		BenchIter:    benchIter,
	}
}

func TestMain(m *testing.M) {
	defer metric.Enable(*recordMetrics)()
	m.Run()
}

func BenchmarkRaft(b *testing.B) {
	util.RunFor(b, "conc", 1, 1, 15, func(b *testing.B, conc int) {
		util.RunFor(b, "bytes", 1, 2, 8, func(b *testing.B, bytes int) {
			benchmarkRaft(b, conc, bytes)
		})
	})
}

func benchmarkRaft(b *testing.B, conc, bytes int) {
	cfg := cfgFromFlags()
	cfg.Epoch = newEpoch()
	p := newPeer(cfg)
	go p.Run()
	defer p.Stop()

	// Wait for the initial leader election to complete.
	becomeLeader(p)

	workers := workload.NewWorkers(workload.Config{
		KeyPrefix: engine.MinDataKey,
		KeyLen:    len(engine.MinDataKey) + 8,
		ValLen:    bytes,
		Workers:   conc,
		Proposals: b.N,
	})

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	var g errgroup.Group
	defer g.Wait()
	for i := range workers {
		worker := workers[i]
		g.Go(func() error {
			c := make(chan bool, 1)
			for prop := worker.NextProposal(); prop != nil; prop = worker.NextProposal() {
				if !p.ProposeWith(prop, c) {
					return errors.New("proposal failed")
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		b.Fatal(err)
	}
}
