package protodb

import (
	"os"
	"testing"
)

// TestMemtableBeforeWAL verifies that Transaction.Apply does NOT make writes
// visible via the memtable until the WAL batch has committed successfully.
// Previously the memtable was mutated eagerly, so a failed WAL write left
// phantom writes visible to readers that would vanish on crash.
func TestMemtableBeforeWAL(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Force every Commit to issue a real write, and swap the WAL handle for
	// one that's already closed so the write errors out.
	engine.wal.flushThreshold = 0
	closed, _ := os.CreateTemp(dir, "closed-handle")
	_ = closed.Close()
	engine.wal.handle = closed

	k, v := Key("phantom"), []byte("phantom-value")
	tx := engine.Transaction()
	tx.Put(k, v)
	if err := tx.Apply(); err == nil {
		t.Fatal("expected Apply to fail with closed WAL handle")
	}

	got, _ := engine.Get(k)
	if got != nil {
		t.Errorf("phantom write: Get returned %q after failed Apply, want nil", got)
	}
}
