package proposal

// Tracker tracks in-flight Raft proposals.
type Tracker struct {
	m map[int64]chan struct{}
}

func MakeTracker() Tracker {
	return Tracker{
		m: make(map[int64]chan struct{}),
	}
}

func (pr *Tracker) Register(enc EncProposal, c chan struct{}) {
	pr.m[enc.GetID()] = c
}

func (pr *Tracker) Finish(id int64) {
	if c, ok := pr.m[id]; ok {
		delete(pr.m, id)
		close(c)
	}
}

func (pr *Tracker) FinishAll() {
	for id, c := range pr.m {
		close(c)
		delete(pr.m, id)
	}
}
