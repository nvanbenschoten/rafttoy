package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/nvanbenschoten/raft-toy/metric"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"golang.org/x/sync/errgroup"
)

// epoch represents a single iteration of a `go bench` loop. Tracking it and
// threading it through Raft messages allows a single instance of `go bench`
// to coordinate Raft state machine resets across a cluster of Go processes.
var epoch int32

func newEpoch() int32 {
	e := epoch
	epoch++
	return e
}

func TestMain(m *testing.M) {
	defer metric.Enable(*recordMetrics)()
	m.Run()
}

func BenchmarkRaft(b *testing.B) {
	runFor(b, "conc", 1, 1, 12, func(b *testing.B, conc int) {
		runFor(b, "bytes", 1, 4, 4, func(b *testing.B, bytes int) {
			benchmarkRaft(b, conc, bytes)
		})
	})
}

func benchmarkRaft(b *testing.B, conc, bytes int) {
	p := newPeer(newEpoch())
	go p.Run()
	defer p.Stop()

	// Wait for the initial leader election to complete.
	becomeLeader(p)

	// Create a single instance of a Raft proposal.
	prop := proposal.Proposal{
		Key: []byte("key"),
		Val: make([]byte, bytes),
	}

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	var g errgroup.Group
	defer g.Wait()
	for i := 0; i < conc; i++ {
		g.Go(func() error {
			iters := b.N / conc
			for j := 0; j < iters; j++ {
				if !p.Propose(prop) {
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

// runFor runs a series of subbenchmark with the specified variable set to
// values starting at start, increasing by 2^exp each step, and consisting
// of n total steps.
func runFor(b *testing.B, name string, start, exp, n int, f func(b *testing.B, i int)) {
	for i := 0; i < n; i++ {
		v := start * (1 << uint(exp*i))
		b.Run(fmt.Sprintf("%s=%d", name, v), func(b *testing.B) {
			f(b, v)
		})
	}
}
