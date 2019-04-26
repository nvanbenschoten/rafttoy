package peer

import (
	"sync"
	"sync/atomic"

	"github.com/nvanbenschoten/raft-toy/proposal"
)

const propBufCap = 1024

type propBuf struct {
	mu   sync.RWMutex
	full sync.Cond
	b    [propBufCap]propBufElem
	i    int32
}

type propBufElem struct {
	enc proposal.EncProposal
	c   chan bool
}

func (b *propBuf) init() {
	b.full.L = b.mu.RLocker()
}

func (b *propBuf) add(e propBufElem) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for {
		n := atomic.AddInt32(&b.i, 1)
		i := int(n - 1)
		if i < len(b.b) {
			b.b[i] = e
			return
		}
		b.full.Wait()
	}
}

func (b *propBuf) flush(f func([]propBufElem)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := int(b.i)
	if i >= len(b.b) {
		i = len(b.b)
		b.full.Broadcast()
	}
	if i > 0 {
		f(b.b[:i])
		b.i = 0
	}
}
