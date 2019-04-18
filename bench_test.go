package main

import (
	"fmt"
	"sync"
	"testing"

	"github.com/nvanbenschoten/raft-toy/util"
)

func init() {
	util.DisableRaftLogging()
}

func BenchmarkRaft(b *testing.B) {
	runFor(b, "conc", 1, 64, 3, func(b *testing.B, conc int) {
		runFor(b, "bytes", 1, 4096, 4, func(b *testing.B, bytes int) {
			benchmarkRaft(b, conc, bytes)
		})
	})
}

func benchmarkRaft(b *testing.B, conc, bytes int) {
	p := newPeer()
	go p.run()
	defer p.stop()

	// Wait for the initial leader election to complete.
	data := make([]byte, bytes)
	for !p.propose(data) {
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
				if !p.propose(data) {
					b.Fatal("proposal failed")
				}
			}
		}()
	}
}

// runFor runs a series of subbenchmark with the specified variable set to
// increasing values between min and max. The value increases by 2^exp each
// iteration.
func runFor(b *testing.B, name string, min, max, exp int, f func(b *testing.B, i int)) {
	for i := min; i <= max; i *= (1 << uint(exp)) {
		b.Run(fmt.Sprintf("%s=%d", name, i), func(b *testing.B) {
			f(b, i)
		})
	}
}
