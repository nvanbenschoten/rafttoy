package main

import (
	"errors"
	"fmt"
	"math/rand"
	"testing"

	"github.com/nvanbenschoten/rafttoy/metric"
	"github.com/nvanbenschoten/rafttoy/proposal"
	"github.com/nvanbenschoten/rafttoy/storage/engine"
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
	runFor(b, "conc", 1, 1, 15, func(b *testing.B, conc int) {
		runFor(b, "bytes", 1, 2, 8, func(b *testing.B, bytes int) {
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
	propBytes := make([]byte, bytes)
	rand.Read(propBytes[:])

	b.ResetTimer()
	b.SetBytes(int64(bytes))
	var g errgroup.Group
	defer g.Wait()
	for i := 0; i < conc; i++ {
		rng := rand.New(rand.NewSource(int64(i)))
		g.Go(func() error {
			keyPrefix := engine.MinDataKey
			prop := proposal.Proposal{
				Key: append(keyPrefix, make([]byte, 8)...),
				Val: propBytes,
			}
			encBuf := make([]byte, proposal.Size(prop))
			c := make(chan bool, 1)

			iters := b.N / conc
			for j := 0; j < iters; j++ {
				rng.Read(prop.Key[len(keyPrefix):])
				enc := proposal.EncodeInto(prop, encBuf)
				if !p.ProposeWith(enc, c) {
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
