package util

import (
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/wal"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type raftStorageImpl struct {
	w wal.Wal
	s storage.Storage
}

// NewRaftStorage creates an implementation of raft.Storage
// from the provided WAL and Storage engine.
func NewRaftStorage(w wal.Wal, s storage.Storage) raft.Storage {
	return &raftStorageImpl{
		w: w,
		s: s,
	}
}

var _ raft.Storage = &raftStorageImpl{}

func (rs *raftStorageImpl) InitialState() (raftpb.HardState, raftpb.ConfState, error) {
	return raftpb.HardState{}, raftpb.ConfState{}, nil
}

func (rs *raftStorageImpl) Entries(lo, hi, maxSize uint64) ([]raftpb.Entry, error) {
	return rs.w.Entries(lo, hi), nil
}

func (rs *raftStorageImpl) Term(i uint64) (uint64, error) {
	return rs.w.Term(i), nil
}

func (rs *raftStorageImpl) LastIndex() (uint64, error) {
	return rs.w.LastIndex(), nil
}

func (rs *raftStorageImpl) FirstIndex() (uint64, error) {
	return rs.w.FirstIndex(), nil
}

func (rs *raftStorageImpl) Snapshot() (raftpb.Snapshot, error) {
	panic("unimplemented")
}
