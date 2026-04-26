package protodb

import (
	"sync"
	"sync/atomic"
)

type blockKey struct {
	hash     sstHash
	blockIdx uint64
}

type blockRef struct {
	data       []byte
	key        blockKey
	referenced atomic.Bool
	refs       atomic.Int32
}

func (r *blockRef) release() {
	if r.refs.Add(-1) == 0 {
		blockBufPut(r.data)
	}
}

type blockCache struct {
	items sync.Map // blockKey -> *blockRef

	ring     []atomic.Pointer[blockRef]
	hand     atomic.Uint64
	bytes    atomic.Int64
	capacity int64
}

func newBlockCache(capacity int64) *blockCache {
	slots := int(capacity / int64(BlockSize))
	slots = max(slots, 16)
	return &blockCache{
		ring:     make([]atomic.Pointer[blockRef], slots),
		capacity: capacity,
	}
}

// Get returns nil/false on miss or on a concurrent-eviction race
// (entry exists in items but its refs already hit 0); both look like
// a miss to the caller.
func (c *blockCache) Get(key blockKey) (*blockRef, bool) {
	v, ok := c.items.Load(key)
	if !ok {
		return nil, false
	}
	r := v.(*blockRef)
	for {
		cur := r.refs.Load()
		if cur == 0 {
			return nil, false
		}
		if r.refs.CompareAndSwap(cur, cur+1) {
			r.referenced.Store(true)
			return r, true
		}
	}
}

func (c *blockCache) Put(key blockKey, data []byte) *blockRef {
	r := &blockRef{key: key, data: data}
	// refs starts at 2: cache + caller, each can drop independently.
	r.refs.Store(2)

	for {
		existing, loaded := c.items.LoadOrStore(key, r)
		if !loaded {
			break
		}
		other := existing.(*blockRef)
		for {
			cur := other.refs.Load()
			if cur == 0 {
				// Existing mid-eviction; retry LoadOrStore once it drains.
				break
			}
			if other.refs.CompareAndSwap(cur, cur+1) {
				blockBufPut(data)
				other.referenced.Store(true)
				return other
			}
		}
	}

	c.bytes.Add(int64(len(data)))

	n := uint32(len(c.ring))
	for sweep := uint32(0); ; sweep++ {
		idx := uint32(c.hand.Add(1)-1) % n
		slot := &c.ring[idx]
		cur := slot.Load()

		if cur == nil {
			// Case 1: install at empty slot, but only when under cap.
			if c.bytes.Load() <= c.capacity || sweep > 2*n {
				if slot.CompareAndSwap(nil, r) {
					return r
				}
			}
			continue
		}

		if cur.referenced.Load() {
			// Case 2: clear referenced bit and advance.
			cur.referenced.Store(false)
			continue
		}

		// Case 3: evict-and-install in one CAS so we don't walk the ring twice.
		if c.bytes.Load() > c.capacity || sweep > n {
			if slot.CompareAndSwap(cur, r) {
				c.bytes.Add(-int64(len(cur.data)))
				c.items.CompareAndDelete(cur.key, cur)
				if cur.refs.Add(-1) == 0 {
					blockBufPut(cur.data)
				}
				return r
			}
		}
	}
}

func (c *blockCache) Stats() (bytes, capacity int64) {
	return c.bytes.Load(), c.capacity
}

// Close releases every entry. Caller must guarantee no live refs.
func (c *blockCache) Close() {
	for i := range c.ring {
		if r := c.ring[i].Load(); r != nil {
			c.ring[i].Store(nil)
			c.items.CompareAndDelete(r.key, r)
			if r.refs.Add(-1) == 0 {
				blockBufPut(r.data)
			}
		}
	}
}

// --- buffer pool ---

// Buckets are multiples of BlockSize: most blocks fit in 1×, large-value
// blocks can reach 64×. Buffers above the largest bucket aren't pooled.
var blockBufBuckets []int
var blockBufPools []*sync.Pool

func init() {
	multipliers := []int{1, 4, 16, 64}
	blockBufBuckets = make([]int, len(multipliers))
	blockBufPools = make([]*sync.Pool, len(multipliers))
	for i, m := range multipliers {
		size := m * BlockSize
		blockBufBuckets[i] = size
		blockBufPools[i] = &sync.Pool{
			New: func() any { b := make([]byte, 0, size); return &b },
		}
	}
}

func blockBufGet(n int) []byte {
	for i, size := range blockBufBuckets {
		if n <= size {
			bp := blockBufPools[i].Get().(*[]byte)
			return (*bp)[:n]
		}
	}
	return make([]byte, n)
}

func blockBufPut(b []byte) {
	c := cap(b)
	for i, size := range blockBufBuckets {
		if c == size {
			full := b[:c]
			blockBufPools[i].Put(&full)
			return
		}
	}
}
