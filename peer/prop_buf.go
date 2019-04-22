package peer

import (
	"sync/atomic"

	"github.com/nvanbenschoten/raft-toy/proposal"
)

const propBufCap = 64

type propBuf struct {
	b [propBufCap]propBufElem
	i int32
}

type propBufElem struct {
	enc proposal.EncProposal
	c   chan bool
}

func (b *propBuf) addRLocked(e propBufElem) bool {
	n := atomic.AddInt32(&b.i, 1)
	i := int(n - 1)
	if i >= len(b.b) {
		return false
	}
	b.b[i] = e
	return true
}

func (b *propBuf) lenWLocked() int {
	return int(b.i)
}

func (b *propBuf) flushWLocked() []propBufElem {
	i := int(b.i)
	if i > len(b.b) {
		i = len(b.b)
	}
	b.i = 0
	return b.b[:i]
}
