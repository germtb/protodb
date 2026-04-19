package protodb

import (
	"bytes"
	"math"
	"math/rand/v2"
	"sync/atomic"
)

const maxHeight = 16

// VisibleAll is a snapshot seqnum that makes every entry visible.
const VisibleAll = uint64(math.MaxUint64)

type skipnode struct {
	key    Key
	seqnum uint64
	value  []byte
	next   [maxHeight]atomic.Pointer[skipnode]
	height int
}

func newSkipnode(key Key, seqnum uint64, value []byte, height int) *skipnode {
	return &skipnode{key: key, seqnum: seqnum, value: value, height: height}
}

type Skiplist struct {
	head     skipnode
	length   atomic.Int64
	byteSize atomic.Int64
}

func NewSkiplist() *Skiplist {
	sl := &Skiplist{}
	sl.head.height = maxHeight
	return sl
}

func randomHeight() int {
	height := 1
	for height < maxHeight && rand.Float32() < 0.5 {
		height++
	}
	return height
}

// compareNodeKey returns negative if (aKey,aSeq) < (bKey,bSeq), 0 if equal,
// positive if greater. Order: user_key ASC, seqnum DESC (higher seqnum first).
func compareNodeKey(aKey Key, aSeq uint64, bKey Key, bSeq uint64) int {
	if c := bytes.Compare(aKey, bKey); c != 0 {
		return c
	}
	if aSeq > bSeq {
		return -1
	}
	if aSeq < bSeq {
		return 1
	}
	return 0
}

// findPredecessors locates the immediate predecessor at every level for the
// composite key (key, seqnum). A predecessor at level L is the last node whose
// composite key strictly precedes (key, seqnum) along the L-th linked list.
func (s *Skiplist) findPredecessors(key Key, seqnum uint64) [maxHeight]*skipnode {
	var predecessors [maxHeight]*skipnode
	node := &s.head

	for level := maxHeight - 1; level >= 0; level-- {
		for {
			next := node.next[level].Load()
			if next == nil || compareNodeKey(next.key, next.seqnum, key, seqnum) >= 0 {
				break
			}
			node = next
		}
		predecessors[level] = node
	}
	return predecessors
}

// insert splices a new node into the list at every level up to its height.
// Callers must ensure (key, seqnum) is unique — the engine enforces this by
// assigning monotonic seqnums under the commit mutex.
func (s *Skiplist) insert(key Key, seqnum uint64, value []byte) {
	predecessors := s.findPredecessors(key, seqnum)
	height := randomHeight()
	newNode := newSkipnode(key, seqnum, value, height)

	for level := 0; level < height; level++ {
		pred := predecessors[level]
		for {
			next := pred.next[level].Load()
			// Walk forward past any nodes that were inserted concurrently and
			// sort before us.
			for next != nil && compareNodeKey(next.key, next.seqnum, key, seqnum) < 0 {
				pred = next
				next = pred.next[level].Load()
			}
			newNode.next[level].Store(next)
			if pred.next[level].CompareAndSwap(next, newNode) {
				break
			}
		}
	}

	s.length.Add(1)
	s.byteSize.Add(int64(len(key) + len(value)))
}

func (s *Skiplist) Put(key Key, value []byte, seqnum uint64) {
	s.insert(key, seqnum, value)
}

func (s *Skiplist) Delete(key Key, seqnum uint64) {
	s.insert(key, seqnum, nil)
}

// Get returns the value for key as seen by a reader at snapshotSeq. It walks
// forward through versions of the same user_key (sorted newest-first) and
// returns the first version whose seqnum ≤ snapshotSeq. A tombstone at that
// position returns ErrDeleted; no matching version returns ErrNotFound.
func (s *Skiplist) Get(key Key, snapshotSeq uint64) ([]byte, error) {
	node := &s.head
	for level := maxHeight - 1; level >= 0; level-- {
		for {
			next := node.next[level].Load()
			if next == nil || bytes.Compare(next.key, key) >= 0 {
				break
			}
			node = next
		}
	}

	candidate := node.next[0].Load()
	for candidate != nil && bytes.Equal(candidate.key, key) {
		if candidate.seqnum <= snapshotSeq {
			if candidate.value == nil {
				return nil, ErrDeleted
			}
			return candidate.value, nil
		}
		candidate = candidate.next[0].Load()
	}
	return nil, ErrNotFound
}

func (s *Skiplist) Len() int {
	return int(s.length.Load())
}

func (s *Skiplist) ByteSize() uint64 {
	return uint64(s.byteSize.Load())
}

// Scan returns an iterator over [lo, hi) restricted to entries visible at
// snapshotSeq. For each user_key, only the newest visible version is yielded
// (tombstones included — callers filter them). A nil lo or hi is unbounded.
func (s *Skiplist) Scan(lo, hi Key, snapshotSeq uint64) *skiplistIterator {
	node := &s.head
	if lo != nil {
		for level := maxHeight - 1; level >= 0; level-- {
			for {
				next := node.next[level].Load()
				if next == nil || bytes.Compare(next.key, lo) >= 0 {
					break
				}
				node = next
			}
		}
	}
	return &skiplistIterator{
		current:     node.next[0].Load(),
		hi:          hi,
		snapshotSeq: snapshotSeq,
	}
}

// Entries iterates the full skiplist with see-all visibility. One version per
// user_key (newest wins). Used by flush.
func (s *Skiplist) Entries() *skiplistIterator {
	return s.Scan(nil, nil, VisibleAll)
}

type skiplistIterator struct {
	current     *skipnode
	hi          Key
	snapshotSeq uint64
	entry       KeyValue
	entrySeqnum uint64
	lastKey     Key
	yielded     bool
}

func (it *skiplistIterator) Next() bool {
	for it.current != nil {
		if it.hi != nil && bytes.Compare(it.current.key, it.hi) >= 0 {
			return false
		}
		if it.yielded && bytes.Equal(it.current.key, it.lastKey) {
			// Older version of a key we've already yielded — skip.
			it.current = it.current.next[0].Load()
			continue
		}
		if it.current.seqnum > it.snapshotSeq {
			// Invisible at this snapshot — skip to look for an older version.
			it.current = it.current.next[0].Load()
			continue
		}
		it.entry = KeyValue{Key: it.current.key, Value: it.current.value}
		it.entrySeqnum = it.current.seqnum
		it.lastKey = it.current.key
		it.yielded = true
		it.current = it.current.next[0].Load()
		return true
	}
	return false
}

func (it *skiplistIterator) Current() KeyValue {
	return it.entry
}

// Seqnum returns the seqnum of the entry last yielded by Next. Only valid
// after Next returns true.
func (it *skiplistIterator) Seqnum() uint64 {
	return it.entrySeqnum
}

func (it *skiplistIterator) Close() error {
	return nil
}
