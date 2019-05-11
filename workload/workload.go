package workload

import (
	"fmt"
	"math"
	"math/rand"

	"github.com/nvanbenschoten/rafttoy/proposal"
)

// Config contains various options to control a proposal workload.
type Config struct {
	KeyPrefix []byte
	KeyLen    int
	ValLen    int
	Workers   int
	Proposals int
}

// Worker is a generator of proposals for one thread.
type Worker struct {
	Config
	workerIdx int

	pEnd   int64
	rng    *rand.Rand
	p      proposal.Proposal
	encBuf []byte
}

// NewWorkers creates the set of workers described by the given Config.
func NewWorkers(cfg Config) []Worker {
	if pLen := len(cfg.KeyPrefix); pLen > cfg.KeyLen {
		panic(fmt.Sprintf(`KeyLen %d must be at least the length of KeyPrefix %d`, cfg.KeyLen, pLen))
	}
	val := make([]byte, cfg.ValLen)
	rand.Read(val)

	pPerWorker := int(math.Ceil(float64(cfg.Proposals) / float64(cfg.Workers)))
	ws := make([]Worker, cfg.Workers)
	for i := range ws {
		pStart, pEnd := i*pPerWorker, (i+1)*pPerWorker
		if pEnd > cfg.Proposals {
			pEnd = cfg.Proposals
		}
		ws[i] = Worker{
			Config:    cfg,
			workerIdx: i,
			pEnd:      int64(pEnd),
			rng:       rand.New(rand.NewSource(int64(i))),
			p: proposal.Proposal{
				ID:  int64(pStart),
				Key: make([]byte, cfg.KeyLen),
				Val: val,
			},
		}
		copy(ws[i].p.Key, cfg.KeyPrefix)
		ws[i].encBuf = make([]byte, proposal.Size(ws[i].p))
	}
	return ws
}

// NextProposal repeatedly returns an encoded proposal to be sent through raft.
// The returned value is only valid until the next call to NextProposal and nil
// is returned when there are no more proposals to process.
//
// NextProposal is not safe for concurrent access.
func (w *Worker) NextProposal() proposal.EncProposal {
	if w.p.ID >= w.pEnd {
		return nil
	}
	rand.Read(w.p.Key[len(w.KeyPrefix):])
	enc := proposal.EncodeInto(w.p, w.encBuf)
	w.p.ID++
	return enc
}
