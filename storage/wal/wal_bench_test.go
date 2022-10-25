package wal_test

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/nvanbenschoten/rafttoy/storage/engine"
	"github.com/nvanbenschoten/rafttoy/storage/wal"
	"github.com/nvanbenschoten/rafttoy/util"
	"github.com/nvanbenschoten/rafttoy/workload"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func benchmarkWalConfig(b *testing.B, batchSize, bytes int, w wal.Wal) {
	workers := workload.NewWorkers(workload.Config{
		KeyPrefix: engine.MinDataKey,
		KeyLen:    len(engine.MinDataKey) + 8,
		ValLen:    bytes,
		Workers:   1,
		Proposals: b.N,
	})
	worker := workers[0]

	var a util.ByteAllocator
	entries := make([]raftpb.Entry, 0, batchSize)
	appendEntries := func() {
		if len(entries) > 0 {
			w.Append(entries, raftpb.HardState{}, true /* sync */)
			entries = entries[:0]
		}
	}

	b.ResetTimer()
	var index uint64
	for prop := worker.NextProposal(); prop != nil; prop = worker.NextProposal() {
		index++
		e := raftpb.Entry{
			Index: index,
			Type:  raftpb.EntryNormal,
		}
		// prop is only valid until NextProposal is called again, so copy it.
		a, e.Data = a.Copy(prop)
		entries = append(entries, e)
		if len(entries) == batchSize {
			appendEntries()
		}
	}
	// Flush out the remaining partial batch.
	appendEntries()

	b.StopTimer()
	b.SetBytes(int64(bytes))
}

func benchmarkWal(b *testing.B, walFn func(root string) wal.Wal) {
	util.RunFor(b, "batch", 1, 3, 4, func(b *testing.B, batchSize int) {
		util.RunFor(b, "bytes", 1, 3, 5, func(b *testing.B, bytes int) {
			dir, err := ioutil.TempDir(``, ``)
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)
			w := walFn(dir)
			benchmarkWalConfig(b, batchSize, bytes, w)
		})
	})
}

func BenchmarkEtcd(b *testing.B) {
	benchmarkWal(b, wal.NewEtcdWal)
}

func BenchmarkPebble(b *testing.B) {
	benchmarkWal(b, func(root string) wal.Wal {
		return engine.NewPebble(root, false /* disableWal */).(wal.Wal)
	})
}
