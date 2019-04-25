package pipeline

import (
	"github.com/nvanbenschoten/raft-toy/metric"
	"github.com/nvanbenschoten/raft-toy/proposal"
	"go.etcd.io/etcd/raft/raftpb"
)

// asyncApplier is a proposal pipeline that applies committed
// Raft entries asynchronously in a separate goroutine. This
// allows it to have a tighter loop around appending log entries
// and sending Raft messages.
//
// The pipeline has an option to acknowledge committed entries
// immediately after learning that they were committed instead
// of waiting until after they have also been applied.
type asyncApplier struct {
	basic
	earlyAck bool
	toApply  chan asyncEvent
}

type asyncEvent struct {
	ents []raftpb.Entry
	sync chan struct{}
	stop chan struct{}
}

// NewAsyncApplier creates a new "async applier" pipeline.
func NewAsyncApplier(earlyAck bool) Pipeline {
	return &asyncApplier{
		earlyAck: earlyAck,
		toApply:  make(chan asyncEvent, 128),
	}
}

func (pl *asyncApplier) RunOnce() {
	rd := pl.n.Ready()
	if pl.earlyAck {
		pl.ackCommittedEnts(rd.CommittedEntries)
	}
	pl.l.Unlock()
	saveToDisk(pl.s, rd.Entries, rd.HardState, rd.MustSync)
	sendMessages(pl.t, pl.epoch, rd.Messages)
	processSnapshot(rd.Snapshot)
	pl.maybeApplyAsync(rd.CommittedEntries)
	pl.l.Lock()
	pl.n.Advance(rd)
}

func (pl *asyncApplier) ackCommittedEnts(ents []raftpb.Entry) {
	for _, e := range ents {
		switch e.Type {
		case raftpb.EntryNormal:
			if len(e.Data) == 0 {
				continue
			}
			ec := proposal.EncProposal(e.Data)
			pl.pt.Finish(ec.GetID())
		case raftpb.EntryConfChange:
		default:
			panic("unexpected")
		}
	}
}

func (pl *asyncApplier) maybeApplyAsync(ents []raftpb.Entry) {
	if len(ents) == 0 {
		return
	}
	hasConfChange := false
	for _, e := range ents {
		if e.Type == raftpb.EntryConfChange {
			hasConfChange = true
			break
		}
	}
	if hasConfChange {
		// Sync with async applier, then apply synchronously.
		syncC := make(chan struct{})
		pl.toApply <- asyncEvent{sync: syncC}
		<-syncC
		applyToStore(pl.n, pl.s, pl.pt, pl.l, ents)
	} else {
		// Send to async applier.
		pl.toApply <- asyncEvent{ents: ents}
	}
}

func (pl *asyncApplier) Start() {
	go func() {
		for ev := range pl.toApply {
			if ev.stop != nil {
				close(ev.stop)
				return
			}
			if ev.sync != nil {
				close(ev.sync)
			}
			if len(ev.ents) > 0 {
				metric.ApplyBatchSizesHistogram.Update(int64(len(ev.ents)))
			}
			for _, e := range ev.ents {
				switch e.Type {
				case raftpb.EntryNormal:
					if len(e.Data) == 0 {
						continue
					}
					pl.s.ApplyEntry(e)
					if !pl.earlyAck {
						ec := proposal.EncProposal(e.Data)
						pl.l.Lock()
						pl.pt.Finish(ec.GetID())
						pl.l.Unlock()
					}
				case raftpb.EntryConfChange:
					panic("unexpected")
				default:
					panic("unexpected")
				}
			}
		}
	}()
}

func (pl *asyncApplier) Stop() {
	stoppedC := make(chan struct{})
	pl.toApply <- asyncEvent{stop: stoppedC}
	<-stoppedC
}
