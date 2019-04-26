package wal

import (
	"math"

	"github.com/nvanbenschoten/raft-toy/util/raftentry"
	"go.etcd.io/etcd/raft/raftpb"
)

const cacheByteLimit = 512 << 20 // 512 MB
const cacheSizeTarget = 2048

// LogCache caches state about the Raft log in-memory to avoid on-disk lookups.
type LogCache struct {
	lastIndex  uint64
	lastTerm   uint64
	truncIndex uint64
	trunc      bool
	ec         *raftentry.Cache
}

// MakeLogCache creates a new Raft log cache. Trunc indicates whether old entries
// in the cache should be periodically evicted.
func MakeLogCache(trunc bool) LogCache {
	return LogCache{trunc: trunc, ec: raftentry.NewCache(cacheByteLimit)}
}

// UpdateOnAppend updates the cache based on the newly-appended entries.
func (c *LogCache) UpdateOnAppend(ents []raftpb.Entry) {
	last := ents[len(ents)-1]
	c.lastIndex = last.Index
	c.lastTerm = last.Term
	c.ec.Add(0, ents, true)
	c.maybeTruncate()
}

func (c *LogCache) maybeTruncate() {
	if c.trunc && c.lastIndex >= c.truncIndex+2*cacheSizeTarget {
		c.truncIndex = c.lastIndex - cacheSizeTarget
		c.ec.Clear(0, c.truncIndex)
	}
}

// Entries returns entries between lo and hi.
func (c *LogCache) Entries(ents []raftpb.Entry, lo, hi uint64) ([]raftpb.Entry, uint64) {
	ents, _, hitIndex, _ := c.ec.Scan(ents, 0, lo, hi, math.MaxUint64)
	return ents, hitIndex
}

// Term attempts to determine the term for the provided log index.
func (c *LogCache) Term(i uint64) (uint64, bool) {
	if c.lastIndex == i && c.lastTerm != 0 {
		return c.lastTerm, true
	}
	if e, ok := c.ec.Get(0, i); ok {
		return e.Term, true
	}
	return 0, false
}

// LastIndex returns the index of the last entry in the log.
func (c *LogCache) LastIndex() uint64 {
	return c.lastIndex
}

// FirstIndex returns the index of the first entry in the log.
func (c *LogCache) FirstIndex() uint64 {
	return 1
}

// Reset resets the cache.
func (c *LogCache) Reset() {
	c.lastIndex = 0
	c.lastTerm = 0
	c.truncIndex = 0
	c.ec.Drop(0)
}
