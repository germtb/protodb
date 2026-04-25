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
