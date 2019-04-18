package pipeline

import (
	"log"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/wal"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type basic struct {
	n *raft.RawNode
	w wal.Wal
	s storage.Storage
	a *proposal.Tracker
}

func NewBasic(n *raft.RawNode, w wal.Wal, s storage.Storage, a *proposal.Tracker) Pipeline {
	return &basic{
		n: n,
		w: w,
		s: s,
		a: a,
	}
}

func (b *basic) Start() {}
func (b *basic) Stop()  {}

func (b *basic) RunOnce() {
	rd := b.n.Ready()
	b.saveToDisk(rd.HardState, rd.Entries, rd.MustSync)
	b.sendMessages(rd.Messages)
	b.processSnapshot(rd.Snapshot)
	b.applyToStore(rd.CommittedEntries)
	b.n.Advance(rd)
}

func (b *basic) sendMessages(msgs []raftpb.Message) {
	if len(msgs) > 0 {
		log.Fatalf("unhandled messages %v", msgs)
	}
}

func (b *basic) saveToDisk(st raftpb.HardState, ents []raftpb.Entry, sync bool) {
	b.w.Append(ents)
	if !raft.IsEmptyHardState(st) {
		b.s.SetHardState(st)
	}
	_ = sync
}

func (b *basic) processSnapshot(sn raftpb.Snapshot) {
	if !raft.IsEmptySnap(sn) {
		log.Fatalf("unhandled snapshot %v", sn)
	}
}

func (b *basic) applyToStore(ents []raftpb.Entry) {
	for _, e := range ents {
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				continue
			}
			ec := proposal.EncProposal(e.Data)
			b.a.Finish(ec.GetID())
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(e.Data)
			b.n.ApplyConfChange(cc)
		default:
			panic("unexpected")
		}
	}
}
