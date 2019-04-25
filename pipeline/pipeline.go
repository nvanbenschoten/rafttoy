package pipeline

import (
	"log"
	"sync"

	"github.com/nvanbenschoten/raft-toy/metric"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/transport"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// Pipeline represents an implementation of a Raft proposal pipeline. It manages
// the interactions between a Raft "raw node" and the various components that
// the Raft "raw node" needs to coordinate with.
type Pipeline interface {
	Init(int32, sync.Locker, *raft.RawNode, storage.Storage, transport.Transport, *proposal.Tracker)
	BumpEpoch(int32, *raft.RawNode)
	Start()
	Stop()
	RunOnce()
}

func saveToDisk(s storage.Storage, ents []raftpb.Entry, st raftpb.HardState, sync bool) {
	if len(ents) > 0 {
		metric.AppendBatchSizesHistogram.Update(int64(len(ents)))
	}
	if as, ok := s.(storage.AtomicStorage); ok {
		as.AppendAndSetHardState(ents, st, sync)
	} else {
		if len(ents) > 0 {
			s.Append(ents)
		}
		if !raft.IsEmptyHardState(st) {
			s.SetHardState(st)
		}
	}
}

func sendMessages(t transport.Transport, epoch int32, msgs []raftpb.Message) {
	if len(msgs) > 0 {
		t.Send(epoch, msgs)
	}
}

func processSnapshot(sn raftpb.Snapshot) {
	if !raft.IsEmptySnap(sn) {
		log.Fatalf("unhandled snapshot %v", sn)
	}
}

func applyToStore(
	n *raft.RawNode, s storage.Storage, pt *proposal.Tracker, l sync.Locker, ents []raftpb.Entry,
) {
	if len(ents) > 0 {
		metric.ApplyBatchSizesHistogram.Update(int64(len(ents)))
	}
	for _, e := range ents {
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				continue
			}
			s.ApplyEntry(e)
			ec := proposal.EncProposal(e.Data)
			l.Lock()
			pt.Finish(ec.GetID())
			l.Unlock()
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(e.Data)
			l.Lock()
			n.ApplyConfChange(cc)
			l.Unlock()
		default:
			panic("unexpected")
		}
	}
}
