package protodb

import "testing"

func TestLRUGetMiss(t *testing.T) {
	cache := newLRU[string, int](3, nil)
	_, ok := cache.Get("missing")
	if ok {
		t.Fatal("expected miss on empty cache")
	}
}

func TestLRUPutAndGet(t *testing.T) {
	cache := newLRU[string, int](3, nil)
	cache.Put("a", 1)
	cache.Put("b", 2)

	got, ok := cache.Get("a")
	if !ok || got != 1 {
		t.Fatalf("Get(a): got %d, %v; want 1, true", got, ok)
	}
	got, ok = cache.Get("b")
	if !ok || got != 2 {
		t.Fatalf("Get(b): got %d, %v; want 2, true", got, ok)
	}
}

func TestLRUOverwrite(t *testing.T) {
	cache := newLRU[string, int](3, nil)
	cache.Put("a", 1)
	cache.Put("a", 2)

	got, ok := cache.Get("a")
	if !ok || got != 2 {
		t.Fatalf("Get(a): got %d, %v; want 2, true", got, ok)
	}
	if cache.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", cache.Len())
	}
}

func TestLRUEvictsOldest(t *testing.T) {
	var evictedKey string
	var evictedVal int
	cache := newLRU[string, int](2, func(key string, value int) {
		evictedKey = key
		evictedVal = value
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3) // should evict "a"

	if evictedKey != "a" || evictedVal != 1 {
		t.Fatalf("evicted: got (%q, %d); want (a, 1)", evictedKey, evictedVal)
	}

	_, ok := cache.Get("a")
	if ok {
		t.Fatal("expected a to be evicted")
	}

	got, ok := cache.Get("b")
	if !ok || got != 2 {
		t.Fatalf("Get(b): got %d, %v; want 2, true", got, ok)
	}
	got, ok = cache.Get("c")
	if !ok || got != 3 {
		t.Fatalf("Get(c): got %d, %v; want 3, true", got, ok)
	}
}

func TestLRUGetPromotes(t *testing.T) {
	var evictedKey string
	cache := newLRU[string, int](2, func(key string, value int) {
		evictedKey = key
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Get("a") // promote "a" — "b" is now the oldest
	cache.Put("c", 3) // should evict "b", not "a"

	if evictedKey != "b" {
		t.Fatalf("evicted: got %q; want b", evictedKey)
	}

	_, ok := cache.Get("b")
	if ok {
		t.Fatal("expected b to be evicted")
	}

	got, ok := cache.Get("a")
	if !ok || got != 1 {
		t.Fatalf("Get(a): got %d, %v; want 1, true", got, ok)
	}
}

func TestLRUOverwritePromotes(t *testing.T) {
	var evictedKey string
	cache := newLRU[string, int](2, func(key string, value int) {
		evictedKey = key
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("a", 10) // overwrite promotes "a" — "b" is now oldest
	cache.Put("c", 3)  // should evict "b"

	if evictedKey != "b" {
		t.Fatalf("evicted: got %q; want b", evictedKey)
	}

	got, ok := cache.Get("a")
	if !ok || got != 10 {
		t.Fatalf("Get(a): got %d, %v; want 10, true", got, ok)
	}
}

func TestLRURemove(t *testing.T) {
	cache := newLRU[string, int](3, nil)
	cache.Put("a", 1)
	cache.Put("b", 2)

	val, ok := cache.Remove("a")
	if !ok || val != 1 {
		t.Fatalf("Remove(a): got %d, %v; want 1, true", val, ok)
	}
	if cache.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", cache.Len())
	}

	_, ok = cache.Get("a")
	if ok {
		t.Fatal("expected a to be removed")
	}
}

func TestLRURemoveMiss(t *testing.T) {
	cache := newLRU[string, int](3, nil)
	_, ok := cache.Remove("missing")
	if ok {
		t.Fatal("expected miss on Remove of nonexistent key")
	}
}

func TestLRURemoveCallsOnEvict(t *testing.T) {
	var evictedKey string
	cache := newLRU[string, int](3, func(key string, value int) {
		evictedKey = key
	})
	cache.Put("a", 1)
	cache.Remove("a")

	if evictedKey != "a" {
		t.Fatalf("Remove should call onEvict, got evictedKey=%q", evictedKey)
	}
}

func TestLRUClear(t *testing.T) {
	var evicted []string
	cache := newLRU[string, int](5, func(key string, value int) {
		evicted = append(evicted, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)

	cache.Clear()

	if cache.Len() != 0 {
		t.Fatalf("Len after Clear: got %d, want 0", cache.Len())
	}
	if len(evicted) != 3 {
		t.Fatalf("evictions: got %d, want 3", len(evicted))
	}
}

func TestLRUClearEmpty(t *testing.T) {
	cache := newLRU[string, int](3, nil)
	cache.Clear() // should not panic
	if cache.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", cache.Len())
	}
}

func TestLRULen(t *testing.T) {
	cache := newLRU[string, int](5, nil)
	if cache.Len() != 0 {
		t.Fatalf("Len: got %d, want 0", cache.Len())
	}

	cache.Put("a", 1)
	cache.Put("b", 2)
	if cache.Len() != 2 {
		t.Fatalf("Len: got %d, want 2", cache.Len())
	}

	cache.Remove("a")
	if cache.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", cache.Len())
	}
}

func TestLRUCapacityOne(t *testing.T) {
	var evictions []string
	cache := newLRU[string, int](1, func(key string, value int) {
		evictions = append(evictions, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2) // evicts a
	cache.Put("c", 3) // evicts b

	if len(evictions) != 2 || evictions[0] != "a" || evictions[1] != "b" {
		t.Fatalf("evictions: got %v; want [a b]", evictions)
	}

	if cache.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", cache.Len())
	}

	got, ok := cache.Get("c")
	if !ok || got != 3 {
		t.Fatalf("Get(c): got %d, %v; want 3, true", got, ok)
	}
}

func TestLRUEvictionOrder(t *testing.T) {
	var evictions []string
	cache := newLRU[string, int](3, func(key string, value int) {
		evictions = append(evictions, key)
	})

	cache.Put("a", 1)
	cache.Put("b", 2)
	cache.Put("c", 3)
	// Access order: a(oldest), b, c(newest)

	cache.Get("a") // promote a: b(oldest), c, a(newest)
	cache.Get("b") // promote b: c(oldest), a, b(newest)

	cache.Put("d", 4) // evicts c
	cache.Put("e", 5) // evicts a
	cache.Put("f", 6) // evicts b

	if len(evictions) != 3 || evictions[0] != "c" || evictions[1] != "a" || evictions[2] != "b" {
		t.Fatalf("evictions: got %v; want [c a b]", evictions)
	}
}

func TestLRUNilOnEvict(t *testing.T) {
	cache := newLRU[string, int](1, nil)
	cache.Put("a", 1)
	cache.Put("b", 2) // evicts a, should not panic with nil onEvict

	if cache.Len() != 1 {
		t.Fatalf("Len: got %d, want 1", cache.Len())
	}
}

func TestLRUManyEntries(t *testing.T) {
	evictCount := 0
	cache := newLRU[int, int](100, func(key int, value int) {
		evictCount++
	})

	for idx := 0; idx < 1000; idx++ {
		cache.Put(idx, idx*10)
	}

	if cache.Len() != 100 {
		t.Fatalf("Len: got %d, want 100", cache.Len())
	}
	if evictCount != 900 {
		t.Fatalf("evictions: got %d, want 900", evictCount)
	}

	// Only the last 100 entries should be present
	for idx := 0; idx < 900; idx++ {
		_, ok := cache.Get(idx)
		if ok {
			t.Fatalf("expected key %d to be evicted", idx)
		}
	}
	for idx := 900; idx < 1000; idx++ {
		got, ok := cache.Get(idx)
		if !ok || got != idx*10 {
			t.Fatalf("Get(%d): got %d, %v; want %d, true", idx, got, ok, idx*10)
		}
	}
}
