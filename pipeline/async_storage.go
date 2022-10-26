package pipeline

import (
	"sync"

	"github.com/nvanbenschoten/rafttoy/metric"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// asyncStorage is a proposal pipeline that appends entries to
// the Raft log and applies committed entries to the state
// machine asynchronously in  separate goroutines. This allows
// it to have a tighter loop around appending log entries and
// sending Raft messages.
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
type asyncStorage struct {
	basic
	earlyAck     bool
	lazyFollower bool
	leader       bool
	// State machine to workers.
	toAppend sliceChan[appendEvent]
	toApply  sliceChan[applyEvent]
	// Workers to state machine.
	resps sliceChan[raftpb.Message]
}

type appendEvent struct {
	msg raftpb.Message
	// synchronization
	sync chan struct{}
	stop chan struct{}
}

type applyEvent struct {
	msg raftpb.Message
	// synchronization
	sync chan struct{}
	stop chan struct{}
}

// NewAsyncStorage creates a new "async storage" pipeline.
func NewAsyncStorage(earlyAck, lazyFollower bool) Pipeline {
	return &asyncStorage{
		earlyAck:     earlyAck,
		lazyFollower: lazyFollower,
	}
}

func (pl *asyncStorage) AsyncStorageWrites() bool {
	return true
}

func (pl *asyncStorage) RunOnce() {
	defer metric.MeasureLat(metric.PipelineLatencyHistogram)()
	for _, m := range pl.resps.take() {
		pl.n.Step(m)
	}
	rd := pl.n.Ready()
	pl.l.Unlock()
	if rd.SoftState != nil {
		pl.leader = rd.SoftState.RaftState == raft.StateLeader
	}
	if pl.earlyAck {
		ackCommittedEnts(pl.pt, rd.CommittedEntries)
	}
	remoteMsgs, localMsgs := splitLocalMsgs(rd.Messages)
	sendMessages(pl.t, pl.epoch, remoteMsgs)
	for _, m := range localMsgs {
		switch m.To {
		case raft.LocalAppendThread:
			pl.toAppend.push(appendEvent{msg: m})
		case raft.LocalApplyThread:
			if pl.leader || !pl.lazyFollower {
				pl.toApply.push(applyEvent{msg: m})
			}
		default:
			panic("unknown target")
		}
	}
	pl.l.Lock()
}

func (pl *asyncStorage) Start() {
	// Start appender goroutine.
	go func() {
		var ents []raftpb.Entry
		for {
			var msgs []raftpb.Message
			var st raftpb.HardState
			var sync, stop chan struct{}

			pl.toAppend.wait()
			for _, ev := range pl.toAppend.take() {
				switch {
				case ev.sync != nil:
					sync = ev.sync
				case ev.stop != nil:
					stop = ev.stop
				default:
					ents = append(ents, ev.msg.Entries...)
					msgs = append(msgs, ev.msg.Responses...)
					if ev.msg.HardState != nil {
						st = *ev.msg.HardState
					}
				}
			}

			saveToDisk(pl.s, ents, st, true /* sync */)
			ents = ents[:0]

			remoteMsgs, localMsgs := splitLocalMsgs(msgs)
			sendMessages(pl.t, pl.epoch, remoteMsgs)

			if len(localMsgs) > 0 {
				pl.resps.push(localMsgs...)
				pl.sig()
			}

			if sync != nil {
				close(sync)
			}
			if stop != nil {
				close(stop)
				return
			}
		}
	}()

	// Start applier goroutine.
	go func() {
		var ents []raftpb.Entry
		for {
			var msgs []raftpb.Message
			var sync, stop chan struct{}

			pl.toApply.wait()
			for _, ev := range pl.toApply.take() {
				switch {
				case ev.sync != nil:
					sync = ev.sync
				case ev.stop != nil:
					stop = ev.stop
				default:
					ents = append(ents, ev.msg.Entries...)
					msgs = append(msgs, ev.msg.Responses...)
				}
			}

			applyToStore(pl.n, pl.s, pl.pt, pl.l, ents, !pl.earlyAck)
			ents = ents[:0]

			remoteMsgs, localMsgs := splitLocalMsgs(msgs)
			sendMessages(pl.t, pl.epoch, remoteMsgs)

			if len(localMsgs) > 0 {
				pl.resps.push(localMsgs...)
				pl.sig()
			}

			if sync != nil {
				close(sync)
			}
			if stop != nil {
				close(stop)
				return
			}
		}
	}()
}

func (pl *asyncStorage) Pause() {
	// Flush toAppend channel.
	syncC := make(chan struct{})
	pl.toAppend.push(appendEvent{sync: syncC})
	<-syncC
	// Flush toApply channel.
	syncC = make(chan struct{})
	pl.toApply.push(applyEvent{sync: syncC})
	<-syncC
	// Clear responses.
	_ = pl.resps.take()
}

func (pl *asyncStorage) Stop() {
	// Stop appender.
	stoppedC := make(chan struct{})
	pl.toAppend.push(appendEvent{stop: stoppedC})
	<-stoppedC
	// Stop applier.
	stoppedC = make(chan struct{})
	pl.toApply.push(applyEvent{stop: stoppedC})
	<-stoppedC
}

// sliceChan is a synchronization channel backed by a slice and a mutex.
type sliceChan[T any] struct {
	mu   sync.Mutex
	cond sync.Cond
	s    []T
}

// push adds the elements to the channel.
func (c *sliceChan[T]) push(m ...T) {
	c.mu.Lock()
	defer c.mu.Unlock()
	prev := len(c.s)
	c.s = append(c.s, m...)
	if prev == 0 {
		if c.cond.L == nil {
			c.cond.L = &c.mu
		}
		c.cond.Signal()
	}
}

// wait blocks for the channel to have at least one element.
func (c *sliceChan[T]) wait() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for len(c.s) == 0 {
		if c.cond.L == nil {
			c.cond.L = &c.mu
		}
		c.cond.Wait()
	}
}

// take grabs the slice from the channel.
func (c *sliceChan[T]) take() []T {
	c.mu.Lock()
	defer c.mu.Unlock()
	res := c.s
	c.s = nil
	return res
}
