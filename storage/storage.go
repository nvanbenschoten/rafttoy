package storage

import (
	"github.com/nvanbenschoten/rafttoy/storage/engine"
	"github.com/nvanbenschoten/rafttoy/storage/wal"
)

// Storage combines the responsibilities of a Raft log and a storage engine.
// It can be implemented by the same underlying structure or by a combination
// of two separate specialized structures.
type Storage interface {
	wal.Wal
	engine.Engine
}

type splitStorage struct {
	wal.Wal
	engine.Engine
}

// CombineWalAndEngine combines a write-ahead log and a storage
// engine to create a Storage implementation.
func CombineWalAndEngine(w wal.Wal, e engine.Engine) Storage {
	return &splitStorage{w, e}
}
