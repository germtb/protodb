package protodb

import (
	"bytes"
	"math"
	"math/rand/v2"
	"sync"
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

// Arena to reduce alloc pressure. Memtable is dropped on flush, so it is easy to GC
type nodeArena struct {
	mu        sync.Mutex
	chunks    [][]skipnode
	chunkSize int
	nextIdx   int
}

const defaultArenaChunkSize = 256

func newNodeArena(chunkSize int) *nodeArena {
	return &nodeArena{chunkSize: chunkSize}
}

func (a *nodeArena) alloc(key Key, seqnum uint64, value []byte, height int) *skipnode {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.chunks) == 0 || a.nextIdx >= a.chunkSize {
		// Fresh chunks come zeroed from `make`, so next[] is already nil.
		a.chunks = append(a.chunks, make([]skipnode, a.chunkSize))
		a.nextIdx = 0
	}
	n := &a.chunks[len(a.chunks)-1][a.nextIdx]
	a.nextIdx++
	n.key = key
	n.seqnum = seqnum
	n.value = value
	n.height = height
	return n
}

type Skiplist struct {
	head     skipnode
	arena    *nodeArena
	length   atomic.Int64
	byteSize atomic.Int64
}

func NewSkiplist() *Skiplist {
	sl := &Skiplist{arena: newNodeArena(defaultArenaChunkSize)}
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
	newNode := s.arena.alloc(key, seqnum, value, height)

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

func (s *Skiplist) Scan(lo, hi Key, snapshotSeq uint64) *skiplistIterator {
	return &skiplistIterator{
		entries: s.materialize(lo, hi, snapshotSeq),
		idx:     -1,
	}
}

func (s *Skiplist) ReverseScan(lo, hi Key, snapshotSeq uint64) *skiplistIterator {
	entries := s.materialize(lo, hi, snapshotSeq)
	return &skiplistIterator{
		entries: entries,
		idx:     len(entries),
		reverse: true,
	}
}

// Entries iterates the full skiplist with see-all visibility. One version per
// user_key (newest wins). Used by flush.
func (s *Skiplist) Entries() *skiplistIterator {
	return s.Scan(nil, nil, VisibleAll)
}

func (s *Skiplist) materialize(lo, hi Key, snapshotSeq uint64) []KeyValue {
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
	current := node.next[0].Load()

	var out []KeyValue
	var lastKey Key
	yielded := false
	for current != nil {
		if hi != nil && bytes.Compare(current.key, hi) >= 0 {
			break
		}
		if yielded && bytes.Equal(current.key, lastKey) {
			// Older version of a key we've already yielded — skip.
			current = current.next[0].Load()
			continue
		}
		if current.seqnum > snapshotSeq {
			// Invisible at this snapshot — keep walking for an older version
			// of the same user_key.
			current = current.next[0].Load()
			continue
		}
		out = append(out, KeyValue{Key: current.key, Value: current.value})
		lastKey = current.key
		yielded = true
		current = current.next[0].Load()
	}
	return out
}

type skiplistIterator struct {
	entries []KeyValue
	idx     int
	reverse bool
}

func (it *skiplistIterator) Next() bool {
	if it.reverse {
		if it.idx <= 0 {
			it.idx = -1
			return false
		}
		it.idx--
		return true
	}
	if it.idx >= len(it.entries)-1 {
		it.idx = len(it.entries)
		return false
	}
	it.idx++
	return true
}

func (it *skiplistIterator) Current() KeyValue {
	return it.entries[it.idx]
}

func (it *skiplistIterator) Close() error {
	return nil
}
