package protodb

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestFileTablePinnedHandleSurvivesEviction verifies that a handle held by
// a caller (refs > 0) is not closed even when LRU eviction would otherwise
// kick it out to make room for other opens. Before the refcount refactor
// the fd would get closed mid-use and subsequent ReadAt returned EBADF.
// TestFileTableEvictsOnlyEnough verifies that going 1 over capacity evicts
// exactly 1 entry, not all non-pinned entries. Before the fix, spillover was
// decremented on error instead of success, causing the loop to drain the
// entire LRU on every single over-capacity open.
func TestFileTableEvictsOnlyEnough(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 4; i++ {
		p := filepath.Join(dir, fmt.Sprintf("f%d", i))
		if err := os.WriteFile(p, []byte("ok"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	ft := newFileTable(DefaultFS, 3) // capacity = 3

	// Fill to capacity: f0, f1, f2. All immediately closed (refs=0).
	for i := 0; i < 3; i++ {
		h, err := ft.getOrOpen(filepath.Join(dir, fmt.Sprintf("f%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		h.Close()
	}

	// Open f3 — goes 1 over capacity, should evict exactly 1 entry (the
	// oldest, f0), leaving f1, f2, f3 in the cache.
	h3, err := ft.getOrOpen(filepath.Join(dir, "f3"))
	if err != nil {
		t.Fatal(err)
	}
	h3.Close()

	ft.mu.Lock()
	n := len(ft.items)
	ft.mu.Unlock()

	if n != 3 {
		t.Errorf("after 1-over-capacity open: got %d cached entries, want 3", n)
	}
}

func TestFileTablePinnedHandleSurvivesEviction(t *testing.T) {
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		p := filepath.Join(dir, fmt.Sprintf("f%d", i))
		if err := os.WriteFile(p, []byte("hello world"), 0644); err != nil {
			t.Fatal(err)
		}
	}

	ft := newFileTable(DefaultFS, 2)

	// Caller gets a handle — refs = 1, in LRU.
	h0, err := ft.getOrOpen(filepath.Join(dir, "f0"))
	if err != nil {
		t.Fatal(err)
	}
	defer h0.Close()

	// Two more opens. h0 is the oldest but it's pinned (refs>0), so
	// evictIfNeeded must skip it — the cache temporarily overfills.
	h1, err := ft.getOrOpen(filepath.Join(dir, "f1"))
	if err != nil {
		t.Fatal(err)
	}
	defer h1.Close()
	h2, err := ft.getOrOpen(filepath.Join(dir, "f2"))
	if err != nil {
		t.Fatal(err)
	}
	defer h2.Close()

	// h0's underlying fd must still be alive.
	buf := make([]byte, 5)
	n, err := h0.ReadAt(buf, 0)
	if err != nil {
		t.Fatalf("pinned handle should still read, got err: %v", err)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("unexpected content: %q", buf[:n])
	}
}
