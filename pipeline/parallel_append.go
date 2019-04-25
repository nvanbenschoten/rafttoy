package pipeline

// parallelAppender is similar to a standard proposal pipeline except
// that it broadcasts MsgApp messages before syncing them to the leader's
// local log. This allows them to be appended in parallel on all peers.
// See github.com/cockroachdb/cockroach/pull/19229.
type parallelAppender struct {
	basic
}

// NewParallelAppender creates a new "basic" pipeline.
func NewParallelAppender() Pipeline {
	return new(parallelAppender)
}

func (pl *parallelAppender) RunOnce() {
	defer measurePipelineLat()()
	rd := pl.n.Ready()
	pl.l.Unlock()
	msgApps, otherMsgs := splitMsgApps(rd.Messages)
	sendMessages(pl.t, pl.epoch, msgApps)
	saveToDisk(pl.s, rd.Entries, rd.HardState, rd.MustSync)
	sendMessages(pl.t, pl.epoch, otherMsgs)
	processSnapshot(rd.Snapshot)
	applyToStore(pl.n, pl.s, pl.pt, pl.l, rd.CommittedEntries)
	pl.l.Lock()
	pl.n.Advance(rd)
}
