package proposal

import "log"

// Tracker tracks in-flight Raft proposals.
type Tracker struct {
	m map[int64]chan bool
}

// MakeTracker creates a new proposal Tracker.
func MakeTracker() Tracker {
	return Tracker{
		m: make(map[int64]chan bool),
	}
}

// Len returns the number of proposals being tracked.
func (pr *Tracker) Len() int {
	return len(pr.m)
}

// Register registers a new proposal with the tracker.
func (pr *Tracker) Register(enc EncProposal, c chan bool) {
	pr.m[enc.GetID()] = c
}

// Finish informs a tracked proposal that it has completed.
func (pr *Tracker) Finish(id int64) {
	if c, ok := pr.m[id]; ok {
		delete(pr.m, id)
		c <- true
	} else {
		log.Fatalf("unknown proposal %d", id)
	}
}

// FinishAll informs all tracked proposal that they have completed.
func (pr *Tracker) FinishAll() {
	for id, c := range pr.m {
		c <- true
		delete(pr.m, id)
	}
}
