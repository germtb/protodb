package protodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	mathrand "math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
)

// bk builds a blockKey from a short string label, padding with zeros into
// the fixed-size sstHash. Tests don't care about hash content, only identity.
func bk(name string, idx uint64) blockKey {
	var h sstHash
	copy(h[:], name)
	return blockKey{hash: h, blockIdx: idx}
}

// putBytes is a convenience for tests that want to install a fixed-content
// block. It allocates a buffer, copies in `src`, and returns the ref from
// Put. Caller must Release.
func putBytes(c *blockCache, k blockKey, src []byte) *blockRef {
	buf := blockBufGet(len(src))
	copy(buf, src)
	return c.Put(k, buf)
}

// putSize allocates a buffer of the given size (zeroed) and Puts it.
func putSize(c *blockCache, k blockKey, n int) *blockRef {
	return c.Put(k, blockBufGet(n))
}

// --- single-threaded correctness ---

func TestBlockCacheMissReturnsFalse(t *testing.T) {
	c := newBlockCache(1 << 20)
	if _, ok := c.Get(bk("a", 0)); ok {
		t.Fatal("expected miss on empty cache")
	}
}

func TestBlockCachePutGetRoundTrip(t *testing.T) {
	c := newBlockCache(1 << 20)
	want := []byte("hello world")
	putBytes(c, bk("a", 0), want).release()

	ref, ok := c.Get(bk("a", 0))
	if !ok {
		t.Fatal("expected hit after put")
	}
	defer ref.release()
	if !bytes.Equal(ref.data, want) {
		t.Errorf("got %q, want %q", ref.data, want)
	}
}

func TestBlockCachePutDuplicateNoOverwrite(t *testing.T) {
	c := newBlockCache(1 << 20)
	first := []byte("first")
	second := []byte("second")
	putBytes(c, bk("a", 0), first).release()
	putBytes(c, bk("a", 0), second).release() // duplicate — should be ignored

	ref, _ := c.Get(bk("a", 0))
	defer ref.release()
	if !bytes.Equal(ref.data, first) {
		t.Errorf("duplicate Put overwrote: got %q, want %q (first wins)", ref.data, first)
	}
}

func TestBlockCacheCapacityRespected(t *testing.T) {
	const cap = 1024
	c := newBlockCache(cap)
	const entrySize = 100
	const entries = 50 // 5000 bytes — well over cap

	for i := 0; i < entries; i++ {
		putSize(c, bk("a", uint64(i)), entrySize).release()
	}

	bytes_, _ := c.Stats()
	if bytes_ > cap+entrySize*2 {
		t.Errorf("bytes=%d exceeds cap=%d by too much", bytes_, cap)
	}
}

func TestBlockCacheClockSecondChance(t *testing.T) {
	c := newBlockCache(60)
	putSize(c, bk("a", 1), 20).release()
	putSize(c, bk("a", 2), 20).release()
	putSize(c, bk("a", 3), 20).release()

	// Touch entry 1 to mark referenced.
	if ref, ok := c.Get(bk("a", 1)); ok {
		ref.release()
	} else {
		t.Fatal("entry 1 missing before eviction")
	}

	// Force eviction.
	putSize(c, bk("a", 4), 20).release()

	if ref, ok := c.Get(bk("a", 1)); ok {
		ref.release()
	} else {
		t.Error("entry 1 was evicted despite being referenced")
	}
}

// --- concurrent correctness ---

func TestBlockCacheConcurrentDataIntegrity(t *testing.T) {
	c := newBlockCache(64 << 10)
	const goroutines = 32
	const iterations = 5000
	const keyspace = 200

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := mathrand.New(mathrand.NewPCG(seed, seed+1))
			for range iterations {
				k := bk("sst", r.Uint64N(keyspace))
				if ref, ok := c.Get(k); ok {
					data := ref.data
					if len(data) < 8 {
						ref.release()
						t.Errorf("short data for %v: len=%d", k, len(data))
						return
					}
					gotIdx := binary.BigEndian.Uint64(data[:8])
					ref.release()
					if gotIdx != k.blockIdx {
						t.Errorf("data integrity violated for %v: got idx %d", k, gotIdx)
						return
					}
				} else {
					size := 64 + int(r.Uint64N(192))
					buf := blockBufGet(size)
					binary.BigEndian.PutUint64(buf[:8], k.blockIdx)
					c.Put(k, buf).release()
				}
			}
		}(uint64(g))
	}
	wg.Wait()

	got, cap_ := c.Stats()
	if got > cap_*3 {
		t.Errorf("cache bytes=%d cap=%d — way over after concurrent load", got, cap_)
	}
}

func TestBlockCacheConcurrentSameKey(t *testing.T) {
	c := newBlockCache(1 << 20)
	const goroutines = 64

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			payload := blockBufGet(128)
			payload[0] = byte(g)
			c.Put(bk("a", 0), payload).release()
		}(g)
	}
	wg.Wait()

	ref, ok := c.Get(bk("a", 0))
	if !ok {
		t.Fatal("no entry after concurrent same-key Puts")
	}
	if len(ref.data) != 128 {
		t.Errorf("data corruption: len=%d, want 128", len(ref.data))
	}
	first := ref.data[0]
	ref.release()

	for range 1000 {
		ref, ok = c.Get(bk("a", 0))
		if !ok {
			t.Fatal("entry disappeared after re-read")
		}
		got := ref.data[0]
		ref.release()
		if got != first {
			t.Errorf("entry flickered: got %d, want %d", got, first)
		}
	}
}

// TestBlockCacheGetHoldsAcrossEviction validates the refcount story: a
// reader holding a ref keeps the underlying buffer alive even when the
// cache evicts the entry. Without refcounting this would be a use-after-
// free against C-heap memory.
func TestBlockCacheGetHoldsAcrossEviction(t *testing.T) {
	c := newBlockCache(64)
	want := []byte("important data")
	putBytes(c, bk("a", 0), want).release()

	ref, ok := c.Get(bk("a", 0))
	if !ok {
		t.Fatal("expected hit")
	}
	defer ref.release()

	// Hammer the cache to force eviction of {"a", 0}. We hold ref, so
	// the C-heap buffer must survive — only the cache's own ref is
	// dropped during eviction.
	for i := 0; i < 100; i++ {
		putSize(c, bk("a", uint64(i + 1)), 32).release()
	}

	if !bytes.Equal(ref.data, want) {
		t.Errorf("buffer mutated under us: got %q", ref.data)
	}
}

func TestBlockCacheConcurrentGetEvict(t *testing.T) {
	c := newBlockCache(8 << 10)
	hot := bk("sst", 42)
	hotPayload := blockBufGet(256)
	binary.BigEndian.PutUint64(hotPayload[:8], hot.blockIdx)
	c.Put(hot, hotPayload).release()

	const readers = 16
	const pressure = 8
	const pressureIters = 5000

	var stop atomic.Bool
	var wg sync.WaitGroup

	for range readers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for !stop.Load() {
				if ref, ok := c.Get(hot); ok {
					data := ref.data
					if len(data) < 8 {
						ref.release()
						t.Error("short read on hot key")
						return
					}
					gotIdx := binary.BigEndian.Uint64(data[:8])
					ref.release()
					if gotIdx != hot.blockIdx {
						t.Errorf("wrong payload on hot key: got %d, want %d", gotIdx, hot.blockIdx)
						return
					}
				}
			}
		}()
	}

	var pwg sync.WaitGroup
	for g := range pressure {
		pwg.Add(1)
		go func(seed uint64) {
			defer pwg.Done()
			r := mathrand.New(mathrand.NewPCG(seed, seed+1))
			for i := 0; i < pressureIters; i++ {
				k := bk("sst", 1000 + r.Uint64N(10000))
				buf := blockBufGet(256)
				binary.BigEndian.PutUint64(buf[:8], k.blockIdx)
				c.Put(k, buf).release()
				if i%50 == 0 {
					hot2 := blockBufGet(256)
					binary.BigEndian.PutUint64(hot2[:8], hot.blockIdx)
					c.Put(hot, hot2).release()
				}
			}
		}(uint64(g))
	}
	pwg.Wait()

	stop.Store(true)
	wg.Wait()
}

func TestBlockCacheNoLeakOnDuplicate(t *testing.T) {
	c := newBlockCache(1 << 20)
	putSize(c, bk("a", 0), 100).release()
	first, _ := c.Stats()
	putSize(c, bk("a", 0), 100).release() // duplicate — should be no-op
	second, _ := c.Stats()
	if first != second {
		t.Errorf("duplicate Put leaked bytes: first=%d second=%d", first, second)
	}
}

func TestBlockCacheManyKeysApproxBounded(t *testing.T) {
	const cap = 100 << 10
	c := newBlockCache(cap)

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 5000; i++ {
				k := bk(fmt.Sprintf("sst-%d", g), uint64(i))
				putSize(c, k, 200).release()
			}
		}(g)
	}
	wg.Wait()

	got, _ := c.Stats()
	const slack = int64(50 << 10)
	if got > cap+slack {
		t.Errorf("cache bytes=%d, cap=%d, allowed=%d", got, cap, cap+slack)
	}
}
