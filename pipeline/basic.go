package pipeline

import (
	"log"
	"sync"

	"github.com/nvanbenschoten/raft-toy/proposal"
	"github.com/nvanbenschoten/raft-toy/storage"
	"github.com/nvanbenschoten/raft-toy/transport"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

type basic struct {
	epoch int32
	n     *raft.RawNode
	s     storage.Storage
	t     transport.Transport
	pt    *proposal.Tracker
}

// NewBasic creates a new "basic" pipeline.
func NewBasic() Pipeline {
	return new(basic)
}

func (b *basic) Init(
	epoch int32, n *raft.RawNode, s storage.Storage, t transport.Transport, pt *proposal.Tracker,
) {
	b.epoch = epoch
	b.n = n
	b.s = s
	b.t = t
	b.pt = pt
}

func (b *basic) Start() {}
func (b *basic) Stop()  {}

func (b *basic) RunOnce(l sync.Locker) {
	rd := b.n.Ready()
	l.Unlock()
	b.saveToDisk(rd.Entries, rd.HardState, rd.MustSync)
	b.sendMessages(rd.Messages)
	b.processSnapshot(rd.Snapshot)
	b.applyToStore(l, rd.CommittedEntries)
	l.Lock()
	b.n.Advance(rd)
}

func (b *basic) sendMessages(msgs []raftpb.Message) {
	if len(msgs) > 0 {
		b.t.Send(b.epoch, msgs)
	}
}

func (b *basic) saveToDisk(ents []raftpb.Entry, st raftpb.HardState, sync bool) {
	if as, ok := b.s.(storage.AtomicStorage); ok {
		as.AppendAndSetHardState(ents, st, sync)
	} else {
		if len(ents) > 0 {
			b.s.Append(ents)
		}
		if !raft.IsEmptyHardState(st) {
			b.s.SetHardState(st)
		}
	}
}

func (b *basic) processSnapshot(sn raftpb.Snapshot) {
	if !raft.IsEmptySnap(sn) {
		log.Fatalf("unhandled snapshot %v", sn)
	}
}

func (b *basic) applyToStore(l sync.Locker, ents []raftpb.Entry) {
	for _, e := range ents {
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				continue
			}
			b.s.ApplyEntry(e)
			ec := proposal.EncProposal(e.Data)
			l.Lock()
			b.pt.Finish(ec.GetID())
			l.Unlock()
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(e.Data)
			b.n.ApplyConfChange(cc)
		default:
			panic("unexpected")
		}
	}
}
