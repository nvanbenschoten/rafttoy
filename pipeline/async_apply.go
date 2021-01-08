package pipeline

import (
	"sync"

	"github.com/nvanbenschoten/rafttoy/metric"
	"github.com/nvanbenschoten/rafttoy/proposal"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
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

	toAppend struct {
		mu    sync.Mutex
		cond  sync.Cond
		slice []appendEvent
	}
	toApply chan asyncEvent
}

type appendEvent struct {
	ents []raftpb.Entry
	sync chan struct{}
	stop chan struct{}
}

type asyncEvent struct {
	ents []raftpb.Entry
	sync chan struct{}
	stop chan struct{}
}

// NewAsyncApplier creates a new "async applier" pipeline.
func NewAsyncApplier(earlyAck, lazyFollower bool) Pipeline {
	pl := &asyncApplier{
		earlyAck:     earlyAck,
		lazyFollower: lazyFollower,
		toApply:      make(chan asyncEvent, 512),
	}
	pl.toAppend.cond.L = &pl.toAppend.mu
	return pl
}

func (pl *asyncApplier) RunOnce() {
	defer measurePipelineLat()()
	rd := pl.n.Ready()
	pl.l.Unlock()
	if rd.SoftState != nil {
		pl.leader = rd.SoftState.RaftState == raft.StateLeader
	}
	if pl.earlyAck {
		ackCommittedEnts(pl.pt, rd.CommittedEntries)
	}
	msgApps, otherMsgs := splitMsgApps(rd.Messages)
	sendMessages(pl.t, pl.epoch, msgApps)
	if rd.MustSync {
		// Flush async appends if the vote or term changed, then synchronously
		// save the update. NOTE: MustSync's definition was changed to ignore
		// entries.
		syncC := make(chan struct{})
		pl.appendAsyncEvent(appendEvent{sync: syncC})
		<-syncC
		saveToDisk(pl.s, rd.Entries, rd.HardState, rd.MustSync)
	} else {
		if len(rd.Entries) > 0 {
			pl.appendAsync(rd.Entries)
		}
		if !raft.IsEmptyHardState(rd.HardState) {
			// HardState.Commit index changed. Update without syncing.
			pl.s.SetHardState(rd.HardState, false /* sync */)
		}
	}
	sendMessages(pl.t, pl.epoch, otherMsgs)
	processSnapshot(rd.Snapshot)
	pl.maybeApplyAsync(rd.CommittedEntries)
	pl.l.Lock()
	pl.n.Advance(rd)
	if rd.MustSync && len(rd.Entries) > 0 {
		pl.n.StableTo(rd.Entries[len(rd.Entries)-1])
	}
	if pl.n.HasReady() {
		pl.signal()
	}
}

func (pl *asyncApplier) appendAsync(ents []raftpb.Entry) {
	pl.appendAsyncEvent(appendEvent{ents: ents})
}

func (pl *asyncApplier) appendAsyncEvent(ev appendEvent) {
	pl.toAppend.mu.Lock()
	empty := len(pl.toAppend.slice) == 0
	pl.toAppend.slice = append(pl.toAppend.slice, ev)
	pl.toAppend.mu.Unlock()
	if empty {
		pl.toAppend.cond.Signal()
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
	// Start appender goroutine.
	go func() {
		var evs []appendEvent
		var ents []raftpb.Entry
		for {
			pl.toAppend.mu.Lock()
			for {
				if len(pl.toAppend.slice) > 0 {
					evs, pl.toAppend.slice = pl.toAppend.slice, evs
					break
				}
				pl.toAppend.cond.Wait()
			}
			pl.toAppend.mu.Unlock()

			for _, ev := range evs {
				ents = append(ents, ev.ents...)
			}

			if len(ents) > 0 {
				pl.s.Append(ents)
				pl.l.Lock()
				pl.n.StableTo(ents[len(ents)-1])
				if pl.n.HasReady() {
					pl.signal()
				}
				pl.l.Unlock()
			}

			for _, ev := range evs {
				if ev.stop != nil {
					close(ev.stop)
					return
				}
				if ev.sync != nil {
					close(ev.sync)
				}
			}

			evs = evs[:0]
			ents = ents[:0]
		}
	}()

	// Start applier goroutine.
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
	// Flush toAppend channel.
	syncC := make(chan struct{})
	pl.appendAsyncEvent(appendEvent{sync: syncC})
	<-syncC
	// Flush toApply channel.
	syncC = make(chan struct{})
	pl.toApply <- asyncEvent{sync: syncC}
	<-syncC
}

func (pl *asyncApplier) Stop() {
	// Flush toAppend channel.
	stoppedC := make(chan struct{})
	pl.appendAsyncEvent(appendEvent{stop: stoppedC})
	<-stoppedC
	// Flush toApply channel.
	stoppedC = make(chan struct{})
	pl.toApply <- asyncEvent{stop: stoppedC}
	<-stoppedC
}
