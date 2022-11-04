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
	props := b.N * batchSize
	worker := workload.NewWorkers(workload.Config{
		KeyPrefix: engine.MinDataKey,
		KeyLen:    len(engine.MinDataKey) + 8,
		ValLen:    bytes,
		Workers:   1,
		Proposals: props,
	})[0]

	entries := make([]raftpb.Entry, 0, props)
	var a util.ByteAllocator
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
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := entries[i*batchSize : (i+1)*batchSize]
		w.Append(batch)
	}
	b.SetBytes(int64(bytes * batchSize))
}

func benchmarkWal(b *testing.B, walFn func(root string) wal.Wal) {
	util.RunFor(b, "batch", 1, 2, 10, func(b *testing.B, batchSize int) {
		util.RunFor(b, "bytes", 1, 3, 6, func(b *testing.B, bytes int) {
			dir, err := ioutil.TempDir("/mnt/data1", "")
			if err != nil {
				b.Fatal(err)
			}
			defer os.RemoveAll(dir)
			w := walFn(dir)
			defer w.CloseWal()
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
