package proposal

// Tracker tracks in-flight Raft proposals.
type Tracker struct {
	m map[int64]chan bool
}

func MakeTracker() Tracker {
	return Tracker{
		m: make(map[int64]chan bool),
	}
}

func (pr *Tracker) Register(enc EncProposal, c chan bool) {
	pr.m[enc.GetID()] = c
}

func (pr *Tracker) Finish(id int64) {
	if c, ok := pr.m[id]; ok {
		delete(pr.m, id)
		c <- true
	}
}

func (pr *Tracker) FinishAll() {
	for id, c := range pr.m {
		c <- true
		delete(pr.m, id)
	}
}
