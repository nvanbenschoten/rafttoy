package pipeline

import (
	"github.com/nvanbenschoten/rafttoy/metric"
	"github.com/nvanbenschoten/rafttoy/proposal"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
)

// asyncApplier is a proposal pipeline that applies committed
// Raft entries asynchronously in a separate goroutine. This
// allows it to have a tighter loop around appending log entries
// and sending Raft messages.
//
// The earlyAck option instructs the pipeline to acknowledge
// committed entries immediately after learning that they were
// committed instead of waiting until after they have also been
// applied.
//
// The lazyFollower option instructs the pipeline to forego entry
// application of Raft followers entirely. This is not quite a
// proposed optimization, but it simulates delayed and/or heavily
// batched entry application on followers, which is.
type asyncApplier struct {
	basic
	earlyAck     bool
	lazyFollower bool
	leader       bool
	toApply      chan asyncEvent
}

type asyncEvent struct {
	ents []raftpb.Entry
	sync chan struct{}
	stop chan struct{}
}

// NewAsyncApplier creates a new "async applier" pipeline.
func NewAsyncApplier(earlyAck, lazyFollower bool) Pipeline {
	return &asyncApplier{
		earlyAck:     earlyAck,
		lazyFollower: lazyFollower,
		toApply:      make(chan asyncEvent, 512),
	}
}

func (pl *asyncApplier) RunOnce() {
	defer measurePipelineLat()()
	rd := pl.n.Ready()
	if rd.SoftState != nil {
		pl.leader = rd.SoftState.RaftState == raft.StateLeader
	}
	if pl.earlyAck {
		pl.ackCommittedEnts(rd.CommittedEntries)
	}
	pl.l.Unlock()
	msgApps, otherMsgs := splitMsgApps(rd.Messages)
	sendMessages(pl.t, pl.epoch, msgApps)
	saveToDisk(pl.s, rd.Entries, rd.HardState, rd.MustSync)
	sendMessages(pl.t, pl.epoch, otherMsgs)
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
			pl.pt.Finish(ec.GetID(), true)
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
		applyToStore(pl.n, pl.s, pl.pt, pl.l, ents, !pl.earlyAck)
	} else if pl.lazyFollower && !pl.leader {
		// Do nothing.
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
						pl.pt.Finish(ec.GetID(), true)
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

func (pl *asyncApplier) Pause() {
	// Flush toApply channel.
	syncC := make(chan struct{})
	pl.toApply <- asyncEvent{sync: syncC}
	<-syncC
}

func (pl *asyncApplier) Stop() {
	stoppedC := make(chan struct{})
	pl.toApply <- asyncEvent{stop: stoppedC}
	<-stoppedC
}
