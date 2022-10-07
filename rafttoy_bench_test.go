package main

import (
	"errors"
	"fmt"
	"sync/atomic"
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
	initFlags()
	defer metric.Enable(*recordMetrics)()
	m.Run()
}

func BenchmarkRaftClosed(b *testing.B) {
	util.RunFor(b, "conc", 1, 1, 15, func(b *testing.B, conc int) {
		util.RunFor(b, "bytes", 1, 2, 8, func(b *testing.B, bytes int) {
			benchmarkRaft(b, conc, 0, bytes)
		})
	})
}

func BenchmarkRaftOpen(b *testing.B) {
	type key struct{ maxrate, bytes int }
	var last key
	logExpect := func(maxrate, bytes int) {
		next := key{maxrate, bytes}
		if next != last && last != (key{}) {
			fmt.Printf("--- expected %.2f MB/s\n", float64(last.maxrate*last.bytes)/1e6)
		}
		last = next
	}

	util.RunFor(b, "maxrate", 128, 1, 13, func(b *testing.B, maxrate int) {
		util.RunFor(b, "bytes", 1, 2, 8, func(b *testing.B, bytes int) {
			benchmarkRaft(b, 0, maxrate, bytes)
			logExpect(maxrate, bytes)
		})
	})
	logExpect(0, 0)
}

func benchmarkRaft(b *testing.B, conc, maxrate, bytes int) {
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
		MaxRate:   maxrate,
		Proposals: b.N,
	})

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	var g errgroup.Group
	var aggDur int64
	for i := range workers {
		worker := workers[i]
		g.Go(func() error {
			c := make(chan bool, 1)
			var dur time.Duration
			for prop := worker.NextProposal(); prop != nil; prop = worker.NextProposal() {
				start := time.Now()
				if !p.ProposeWith(prop, c) {
					return errors.New("proposal failed")
				}
				dur += time.Since(start)
			}
			atomic.AddInt64(&aggDur, dur.Nanoseconds())
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		b.Fatal(err)
	}
	b.ReportMetric(float64(aggDur)/float64(b.N), "ns/op")
}
