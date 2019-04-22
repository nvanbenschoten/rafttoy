package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/util"
)

func init() {
	util.DisableRaftLogging()
}

func BenchmarkRaft(b *testing.B) {
	runFor(b, "conc", 1, 3, 5, func(b *testing.B, conc int) {
		runFor(b, "bytes", 1, 4, 4, func(b *testing.B, bytes int) {
			benchmarkRaft(b, conc, bytes)
		})
	})
}

func benchmarkRaft(b *testing.B, conc, bytes int) {
	p := newPeer()
	go p.Run()
	defer p.Stop()

	// Wait for the initial leader election to complete.
	prop := proposal.Proposal{
		Key: []byte("key"),
		Val: make([]byte, bytes),
	}
	for !p.Propose(prop) {
	}

	b.ResetTimer()
	b.SetBytes(int64(conc * bytes))
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < b.N; j++ {
				if !p.Propose(prop) {
					b.Fatal("proposal failed")
				}
			}
		}()
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
