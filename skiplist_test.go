package protodb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func skipKey(n uint64) Key {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, n)
	return buf
}

func TestSkiplistPutGet(t *testing.T) {
	sl := NewSkiplist()
	sl.Put(skipKey(1), []byte("one"), 1)
	sl.Put(skipKey(2), []byte("two"), 2)
	sl.Put(skipKey(3), []byte("three"), 3)

	for _, tc := range []struct {
		key  uint64
		want string
	}{
		{1, "one"},
		{2, "two"},
		{3, "three"},
	} {
		got, err := sl.Get(skipKey(tc.key), VisibleAll)
		if err != nil {
			t.Fatalf("Get(%d): %v", tc.key, err)
		}
		if string(got) != tc.want {
			t.Fatalf("Get(%d): got %q, want %q", tc.key, got, tc.want)
		}
	}
}

func TestSkiplistGetMissing(t *testing.T) {
	sl := NewSkiplist()
	sl.Put(skipKey(1), []byte("one"), 1)

	_, err := sl.Get(skipKey(99), VisibleAll)
	if err != ErrNotFound {
		t.Fatalf("Get(99): got err=%v, want ErrNotFound", err)
	}
}

func TestSkiplistOverwrite(t *testing.T) {
	sl := NewSkiplist()
	sl.Put(skipKey(1), []byte("first"), 1)
	sl.Put(skipKey(1), []byte("second"), 2)

	got, err := sl.Get(skipKey(1), VisibleAll)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "second" {
		t.Fatalf("Get(1): got %q, want %q", got, "second")
	}

	// Both versions are preserved — the skiplist is append-only per seqnum.
	if sl.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", sl.Len())
	}
}

func TestSkiplistDelete(t *testing.T) {
	sl := NewSkiplist()
	sl.Put(skipKey(1), []byte("one"), 1)
	sl.Delete(skipKey(1), 2)

	_, err := sl.Get(skipKey(1), VisibleAll)
	if err != ErrDeleted {
		t.Fatalf("Get(1) after delete: got err=%v, want ErrDeleted", err)
	}
}

func TestSkiplistDeleteNonexistent(t *testing.T) {
	sl := NewSkiplist()
	sl.Delete(skipKey(5), 1)

	_, err := sl.Get(skipKey(5), VisibleAll)
	if err != ErrDeleted {
		t.Fatalf("Get(5) after delete: got err=%v, want ErrDeleted", err)
	}
}

func TestSkiplistOrdering(t *testing.T) {
	sl := NewSkiplist()
	var seq uint64
	// Insert out of order
	for _, k := range []uint64{50, 10, 30, 20, 40} {
		seq++
		sl.Put(skipKey(k), []byte(fmt.Sprintf("v%d", k)), seq)
	}

	iter := sl.Entries()
	var keys []uint64
	for iter.Next() {
		k := binary.BigEndian.Uint64(iter.Current().Key)
		keys = append(keys, k)
	}

	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Fatalf("not sorted: keys[%d]=%d <= keys[%d]=%d", i, keys[i], i-1, keys[i-1])
		}
	}

	if len(keys) != 5 {
		t.Fatalf("Entries: got %d keys, want 5", len(keys))
	}
}

func TestSkiplistScan(t *testing.T) {
	sl := NewSkiplist()
	for i := uint64(0); i < 100; i++ {
		sl.Put(skipKey(i), []byte(fmt.Sprintf("v%d", i)), uint64(i+1))
	}

	iter := sl.Scan(skipKey(20), skipKey(30), VisibleAll)
	var keys []uint64
	for iter.Next() {
		k := binary.BigEndian.Uint64(iter.Current().Key)
		keys = append(keys, k)
	}

	if len(keys) != 10 {
		t.Fatalf("Scan(20,30): got %d entries, want 10", len(keys))
	}
	if keys[0] != 20 {
		t.Fatalf("Scan(20,30): first key=%d, want 20", keys[0])
	}
	if keys[len(keys)-1] != 29 {
		t.Fatalf("Scan(20,30): last key=%d, want 29", keys[len(keys)-1])
	}
}

func TestSkiplistScanAll(t *testing.T) {
	sl := NewSkiplist()
	for i := uint64(0); i < 10; i++ {
		sl.Put(skipKey(i), []byte(fmt.Sprintf("v%d", i)), uint64(i+1))
	}

	iter := sl.Scan(nil, nil, VisibleAll)
	count := 0
	for iter.Next() {
		count++
	}
	if count != 10 {
		t.Fatalf("Scan(nil,nil): got %d entries, want 10", count)
	}
}

func TestSkiplistScanIncludesTombstones(t *testing.T) {
	sl := NewSkiplist()
	sl.Put(skipKey(1), []byte("one"), 1)
	sl.Put(skipKey(2), []byte("two"), 2)
	sl.Put(skipKey(3), []byte("three"), 3)
	sl.Delete(skipKey(2), 4)

	iter := sl.Entries()
	count := 0
	for iter.Next() {
		count++
		if bytes.Equal(iter.Current().Key, skipKey(2)) {
			if iter.Current().Value != nil {
				t.Fatal("tombstone should have nil value")
			}
		}
	}
	// Entries dedupes by user_key; key 2's newest visible version is the tombstone.
	if count != 3 {
		t.Fatalf("Entries after delete: got %d, want 3", count)
	}
}

func TestSkiplistByteSize(t *testing.T) {
	sl := NewSkiplist()
	sl.Put([]byte("key1"), []byte("val1"), 1)
	sl.Put([]byte("key2"), []byte("val2"), 2)

	expected := uint64(4 + 4 + 4 + 4)
	if sl.ByteSize() != expected {
		t.Fatalf("ByteSize: got %d, want %d", sl.ByteSize(), expected)
	}

	// Append a new version of key1 — both versions contribute to ByteSize.
	sl.Put([]byte("key1"), []byte("longer_val"), 3)
	expected = uint64(4+4) + uint64(4+4+4+10) // key2 pair + two key1 pairs
	if sl.ByteSize() != expected {
		t.Fatalf("ByteSize after append: got %d, want %d", sl.ByteSize(), expected)
	}
}

func TestSkiplistManyEntries(t *testing.T) {
	sl := NewSkiplist()
	count := 10000

	for i := 0; i < count; i++ {
		sl.Put(skipKey(uint64(i)), []byte(fmt.Sprintf("value-%d", i)), uint64(i+1))
	}

	if sl.Len() != count {
		t.Fatalf("Len: got %d, want %d", sl.Len(), count)
	}

	for i := 0; i < count; i++ {
		got, err := sl.Get(skipKey(uint64(i)), VisibleAll)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		want := fmt.Sprintf("value-%d", i)
		if string(got) != want {
			t.Fatalf("Get(%d): got %q, want %q", i, got, want)
		}
	}

	iter := sl.Entries()
	prev := uint64(0)
	scanned := 0
	for iter.Next() {
		k := binary.BigEndian.Uint64(iter.Current().Key)
		if scanned > 0 && k <= prev {
			t.Fatalf("not sorted at entry %d: %d <= %d", scanned, k, prev)
		}
		prev = k
		scanned++
	}
	if scanned != count {
		t.Fatalf("Entries: got %d, want %d", scanned, count)
	}
}

func TestSkiplistConcurrentReads(t *testing.T) {
	sl := NewSkiplist()
	for i := uint64(0); i < 1000; i++ {
		sl.Put(skipKey(i), []byte(fmt.Sprintf("v%d", i)), uint64(i+1))
	}

	var wg sync.WaitGroup
	for goroutine := 0; goroutine < 10; goroutine++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < 1000; i++ {
				got, err := sl.Get(skipKey(i), VisibleAll)
				if err != nil {
					t.Errorf("Get(%d): %v", i, err)
					return
				}
				want := fmt.Sprintf("v%d", i)
				if string(got) != want {
					t.Errorf("Get(%d): got %q, want %q", i, got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestSkiplistConcurrentWrites(t *testing.T) {
	sl := NewSkiplist()
	var wg sync.WaitGroup
	var seq atomic.Uint64

	for goroutine := 0; goroutine < 10; goroutine++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				k := uint64(base*100 + i)
				sl.Put(skipKey(k), []byte(fmt.Sprintf("v%d", k)), seq.Add(1))
			}
		}(goroutine)
	}
	wg.Wait()

	for i := uint64(0); i < 1000; i++ {
		got, err := sl.Get(skipKey(i), VisibleAll)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		want := fmt.Sprintf("v%d", i)
		if string(got) != want {
			t.Fatalf("Get(%d): got %q, want %q", i, got, want)
		}
	}
}

func TestSkiplistConcurrentReadsDuringWrites(t *testing.T) {
	sl := NewSkiplist()
	for i := uint64(0); i < 500; i++ {
		sl.Put(skipKey(i), []byte(fmt.Sprintf("v%d", i)), uint64(i+1))
	}

	var wg sync.WaitGroup
	var seq atomic.Uint64
	seq.Store(500)

	for goroutine := 0; goroutine < 5; goroutine++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				k := uint64(500 + base*100 + i)
				sl.Put(skipKey(k), []byte(fmt.Sprintf("v%d", k)), seq.Add(1))
			}
		}(goroutine)
	}

	for goroutine := 0; goroutine < 5; goroutine++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := uint64(0); i < 500; i++ {
				got, err := sl.Get(skipKey(i), VisibleAll)
				if err != nil {
					t.Errorf("Get(%d): %v", i, err)
					return
				}
				want := fmt.Sprintf("v%d", i)
				if string(got) != want {
					t.Errorf("Get(%d): got %q, want %q", i, got, want)
					return
				}
			}
		}()
	}

	wg.Wait()
}

func TestSkiplistConcurrentPutSameKey(t *testing.T) {
	sl := NewSkiplist()
	var wg sync.WaitGroup
	var seq atomic.Uint64

	// 10 goroutines all writing to the same key concurrently — each Put gets
	// a unique seqnum, so all 1000 versions are preserved.
	for goroutine := 0; goroutine < 10; goroutine++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for round := 0; round < 100; round++ {
				sl.Put(skipKey(42), []byte(fmt.Sprintf("g%d-r%d", id, round)), seq.Add(1))
			}
		}(goroutine)
	}
	wg.Wait()

	if sl.Len() != 1000 {
		t.Fatalf("Len: got %d, want 1000", sl.Len())
	}

	// Get returns the value stamped with the highest seqnum (the last writer).
	got, err := sl.Get(skipKey(42), VisibleAll)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("Get returned empty value")
	}

	// Verify skiplist walk yields exactly 1000 nodes at level 0 for key 42.
	count := 0
	node := sl.head.next[0].Load()
	for node != nil {
		if bytes.Equal(node.key, skipKey(42)) {
			count++
		}
		node = node.next[0].Load()
	}
	if count != 1000 {
		t.Fatalf("level-0 nodes for key 42: got %d, want 1000", count)
	}
}

func TestBulkGetDuringFlush(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	for i := uint64(0); i < 100; i++ {
		engine.Put(skipKey(i), []byte(fmt.Sprintf("v%d", i)))
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.Flush()
	}()

	keys := make([]Key, 100)
	for i := uint64(0); i < 100; i++ {
		keys[i] = skipKey(i)
	}

	for attempt := 0; attempt < 100; attempt++ {
		results, err := engine.BulkGet(keys)
		if err != nil {
			t.Fatalf("BulkGet: %v", err)
		}
		for i, result := range results {
			if result == nil {
				t.Fatalf("BulkGet: key %d returned nil on attempt %d", i, attempt)
			}
		}
	}

	wg.Wait()
}

func TestSkiplistConcurrentScanDuringWrites(t *testing.T) {
	sl := NewSkiplist()
	for i := uint64(0); i < 100; i++ {
		sl.Put(skipKey(i), []byte(fmt.Sprintf("v%d", i)), uint64(i+1))
	}

	var wg sync.WaitGroup
	var seq atomic.Uint64
	seq.Store(100)

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := uint64(100); i < 200; i++ {
			sl.Put(skipKey(i), []byte(fmt.Sprintf("v%d", i)), seq.Add(1))
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := 0; round < 10; round++ {
			iter := sl.Scan(skipKey(0), skipKey(100), VisibleAll)
			var prev uint64
			first := true
			for iter.Next() {
				k := binary.BigEndian.Uint64(iter.Current().Key)
				if !first && k <= prev {
					t.Errorf("scan not sorted: %d <= %d", k, prev)
					return
				}
				prev = k
				first = false
			}
		}
	}()

	wg.Wait()
}

