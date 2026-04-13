package protodb

import "testing"

// TestTombstoneResurrection verifies that a Delete for a key that lives in L1
// is preserved across L0->L1 compaction. Previously the mergeIterator used by
// compaction filtered out tombstones, so the deletion was lost during
// compaction and the key "resurrected".
func TestTombstoneResurrection(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(key(1), []byte("v"))
	engine.Flush()
	engine.Compact()
	engine.Delete(key(1))
	engine.Flush()
	engine.Put(key(2), []byte("other"))
	engine.Flush()
	engine.Compact()
	got, _ := engine.Get(key(1))
	if got != nil {
		t.Errorf("resurrection: got %q, want nil", got)
	}
}
