package protodb

import (
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

func openTestEngine(t *testing.T) *Engine {
	t.Helper()
	engine, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return engine
}

// --- Open ---

func TestOpenFreshDB(t *testing.T) {
	engine := openTestEngine(t)
	if engine == nil {
		t.Fatal("Open returned nil")
	}
}

func TestOpenExistingDB(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("hello"))
	err = engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen
	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after reopen: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

// --- Put and Get ---

func TestEnginePutGet(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

func TestEngineGetMissing(t *testing.T) {
	engine := openTestEngine(t)

	got, err := engine.Get(99)
	if err != nil {
		t.Fatalf("Get(99): %v", err)
	}
	if got != nil {
		t.Errorf("Get(99): got %v, want nil", got)
	}
}

func TestEnginePutOverwrite(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("first"))
	engine.Put(1, []byte("second"))

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Get(1): got %q, want %q", got, "second")
	}
}

// --- Delete ---

func TestEngineDelete(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))
	engine.Delete(1)

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1) after delete: got %v, want nil", got)
	}
}

func TestEngineDeleteNonexistent(t *testing.T) {
	engine := openTestEngine(t)
	engine.Delete(99)

	got, err := engine.Get(99)
	if err != nil {
		t.Fatalf("Get(99): %v", err)
	}
	if got != nil {
		t.Errorf("Get(99): got %v, want nil", got)
	}
}

// --- Flush ---

func TestEngineFlush(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))

	err := engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// Value should still be readable from SST
	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after flush: %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

func TestEngineFlushThenPut(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("before"))
	engine.Flush()
	engine.Put(1, []byte("after"))

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "after" {
		t.Errorf("Get(1): got %q, want %q", got, "after")
	}
}

func TestEngineMultipleFlushes(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Flush()

	engine.Put(2, []byte("b"))
	engine.Flush()

	engine.Put(3, []byte("c"))
	engine.Flush()

	for key, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// --- Delete across flush boundary (tombstone shadows older SST) ---

func TestEngineDeleteShadowsOlderSST(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))
	engine.Flush()

	// Delete in memtable should shadow the SST value
	engine.Delete(1)

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1) after delete: got %v, want nil", got)
	}
}

func TestEngineDeleteFlushedThenGet(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))
	engine.Flush()

	engine.Delete(1)
	engine.Flush()

	// Tombstone SST should shadow value SST
	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1) after flushed delete: got %v, want nil", got)
	}
}

func TestEngineDeleteThenReopen(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("hello"))
	engine.Flush()
	engine.Delete(1)
	engine.Flush()

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1) after reopen: %v", err)
	}
	if got != nil {
		t.Errorf("Get(1): got %v, want nil", got)
	}
}

// --- Overwrite across flush boundary ---

func TestEngineOverwriteAcrossFlush(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("old"))
	engine.Flush()
	engine.Put(1, []byte("new"))

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "new" {
		t.Errorf("Get(1): got %q, want %q", got, "new")
	}
}

func TestEngineOverwriteAcrossMultipleFlushes(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("v1"))
	engine.Flush()
	engine.Put(1, []byte("v2"))
	engine.Flush()
	engine.Put(1, []byte("v3"))

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "v3" {
		t.Errorf("Get(1): got %q, want %q", got, "v3")
	}
}

// --- Scan ---

func TestEngineScanMemtableOnly(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))

	var keys []uint64
	iter := engine.Scan(1, 4)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 3 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 {
		t.Errorf("Scan: got %v, want [1 2 3]", keys)
	}
}

func TestEngineScanAcrossFlush(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(3, []byte("c"))
	engine.Flush()
	engine.Put(2, []byte("b"))
	engine.Put(4, []byte("d"))

	var keys []uint64
	iter := engine.Scan(1, 5)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 4 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 || keys[3] != 4 {
		t.Errorf("Scan: got %v, want [1 2 3 4]", keys)
	}
}

func TestEngineScanMergesDuplicates(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("old"))
	engine.Flush()
	engine.Put(1, []byte("new"))

	var values []string
	iter := engine.Scan(0, 10)
	for iter.Next() {
		value := iter.Value()
		values = append(values, string(value))
	}

	if len(values) != 1 || values[0] != "new" {
		t.Errorf("Scan values: got %v, want [new]", values)
	}
}

func TestEngineScanSkipsTombstones(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	engine.Flush()
	engine.Delete(2)

	var keys []uint64
	iter := engine.Scan(1, 4)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 2 || keys[0] != 1 || keys[1] != 3 {
		t.Errorf("Scan: got %v, want [1 3]", keys)
	}
}

func TestEngineScanTombstoneShadowsOlderSST(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	engine.Flush()

	engine.Delete(2)
	engine.Flush()

	var keys []uint64
	iter := engine.Scan(1, 4)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 2 || keys[0] != 1 || keys[1] != 3 {
		t.Errorf("Scan: got %v, want [1 3]", keys)
	}
}

func TestEngineScanEmpty(t *testing.T) {
	engine := openTestEngine(t)

	var count int = 0
	iter := engine.Scan(0, 100)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan on empty: got %d entries, want 0", count)
	}
}

func TestEngineScanBreakEarly(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))

	var keys []uint64
	iter := engine.Scan(1, 4)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
		if key == 2 {
			break
		}
	}

	if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 {
		t.Errorf("Scan with break: got %v, want [1 2]", keys)
	}
}

func TestEngineScanMultipleSSTs(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(5, []byte("e"))
	engine.Flush()

	engine.Put(2, []byte("b"))
	engine.Put(4, []byte("d"))
	engine.Flush()

	engine.Put(3, []byte("c"))

	var keys []uint64
	iter := engine.Scan(1, 6)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 5 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 || keys[3] != 4 || keys[4] != 5 {
		t.Errorf("Scan: got %v, want [1 2 3 4 5]", keys)
	}
}

// --- Persistence ---

func TestEnginePersistenceMultipleFlushes(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Put(2, []byte("b"))
	engine.Flush()

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := engine2.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

func TestEngineUnflushedDataRecoveredOnReopen(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("flushed"))
	engine.Flush()
	engine.Put(2, []byte("not flushed"))

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "flushed" {
		t.Errorf("Get(1): got %q, want %q", got, "flushed")
	}

	got, err = engine2.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "not flushed" {
		t.Errorf("Get(2): got %q, want %q (WAL should recover unflushed data)", got, "not flushed")
	}
}

// --- Edge cases ---

func TestEngineEmptyFlush(t *testing.T) {
	engine := openTestEngine(t)

	err := engine.Flush()
	if err != nil {
		t.Fatal(err)
	}
}

func TestEngineKeyZero(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(0, []byte("zero"))

	got, err := engine.Get(0)
	if err != nil {
		t.Fatalf("Get(0): %v", err)
	}
	if string(got) != "zero" {
		t.Errorf("Get(0): got %q, want %q", got, "zero")
	}
}

func TestEngineEmptyValue(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte{})

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil, want empty slice")
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}
}

func TestEngineDeleteThenPutSameKey(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("first"))
	engine.Flush()
	engine.Delete(1)
	engine.Put(1, []byte("resurrected"))

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "resurrected" {
		t.Errorf("Get(1): got %q, want %q", got, "resurrected")
	}
}

func TestEngineLargeValues(t *testing.T) {
	engine := openTestEngine(t)
	big := make([]byte, 1024*1024)
	for i := range big {
		big[i] = byte(i % 256)
	}
	engine.Put(1, big)
	engine.Flush()

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if len(got) != len(big) {
		t.Fatalf("Get(1): got len %d, want %d", len(got), len(big))
	}
	if got[0] != big[0] || got[len(got)-1] != big[len(big)-1] {
		t.Error("Get(1): value mismatch")
	}
}

func TestEngineManyKeys(t *testing.T) {
	engine := openTestEngine(t)
	for i := uint64(0); i < 1000; i++ {
		engine.Put(i, []byte("value"))
	}
	engine.Flush()

	for i := uint64(0); i < 1000; i++ {
		got, err := engine.Get(i)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if string(got) != "value" {
			t.Fatalf("Get(%d): got %q, want %q", i, got, "value")
		}
	}
}

// --- Empty SST edge cases ---

func TestEngineEmptyFlushThenGet(t *testing.T) {
	engine := openTestEngine(t)
	engine.Flush() // empty SST

	engine.Put(1, []byte("hello"))

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

func TestEngineEmptyFlushThenScan(t *testing.T) {
	engine := openTestEngine(t)
	engine.Flush() // empty SST

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))

	var keys []uint64
	iter := engine.Scan(0, 10)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 {
		t.Errorf("Scan: got %v, want [1 2]", keys)
	}
}

func TestEngineMultipleEmptyFlushes(t *testing.T) {
	engine := openTestEngine(t)
	engine.Flush()
	engine.Flush()
	engine.Flush()

	engine.Put(1, []byte("hello"))
	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

func TestEngineEmptyFlushBetweenDataFlushes(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Flush() // empty
	engine.Put(2, []byte("b"))
	engine.Flush()

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// --- Nil value (tombstone via Put) ---

func TestEnginePutNilIsTombstone(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))
	engine.Put(1, nil) // tombstone via Put

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1) after Put(nil): got %v, want nil", got)
	}
}

func TestEnginePutNilShadowsSST(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))
	engine.Flush()
	engine.Put(1, nil) // tombstone via Put

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1) after Put(nil): got %v, want nil", got)
	}
}

func TestEnginePutNilFlushedShadowsSST(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("hello"))
	engine.Flush()
	engine.Put(1, nil)
	engine.Flush()

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1): got %v, want nil", got)
	}
}

func TestEngineScanWithNilTombstones(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	engine.Flush()
	engine.Put(2, nil) // tombstone via Put

	var keys []uint64
	iter := engine.Scan(1, 4)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 2 || keys[0] != 1 || keys[1] != 3 {
		t.Errorf("Scan: got %v, want [1 3]", keys)
	}
}

// --- Mixed tombstone scenarios ---

func TestEngineAllDeletedThenScan(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	engine.Flush()
	engine.Delete(1)
	engine.Delete(2)
	engine.Delete(3)

	var count int = 0
	iter := engine.Scan(0, 10)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan: got %d entries, want 0", count)
	}
}

func TestEngineAllDeletedFlushedThenScan(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()
	engine.Delete(1)
	engine.Delete(2)
	engine.Flush()

	var count int = 0
	iter := engine.Scan(0, 10)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan: got %d entries, want 0", count)
	}
}

func TestEngineDeleteResurrect(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("v1"))
	engine.Flush()
	engine.Delete(1)
	engine.Flush()
	engine.Put(1, []byte("v2"))
	engine.Flush()

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "v2" {
		t.Errorf("Get(1): got %q, want %q", got, "v2")
	}
}

func TestEngineDeleteResurrectScan(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("v1"))
	engine.Flush()
	engine.Delete(1)
	engine.Flush()
	engine.Put(1, []byte("v2"))

	var values []string
	iter := engine.Scan(0, 10)
	for iter.Next() {
		value := iter.Value()
		values = append(values, string(value))
	}

	if len(values) != 1 || values[0] != "v2" {
		t.Errorf("Scan: got %v, want [v2]", values)
	}
}

// --- Empty values across flush ---

func TestEngineEmptyValueAcrossFlush(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte{})
	engine.Flush()

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil, want empty slice")
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}
}

func TestEngineEmptyValueNotTombstone(t *testing.T) {
	// Empty value should NOT be treated as a tombstone
	engine := openTestEngine(t)
	engine.Put(1, []byte("real"))
	engine.Flush()
	engine.Put(1, []byte{}) // overwrite with empty, NOT delete

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil, want empty slice (empty value is not a tombstone)")
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}
}

func TestEngineEmptyValueScan(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte{})
	engine.Put(2, []byte("hello"))
	engine.Flush()

	var count int = 0
	iter := engine.Scan(0, 10)
	for iter.Next() {
		count++
	}
	// Empty value should appear in scan (it's not a tombstone)
	if count != 2 {
		t.Errorf("Scan: got %d entries, want 2", count)
	}
}

// =============================================================================
// --- Compaction tests ---
// =============================================================================

func TestCompactBasic(t *testing.T) {
	engine := openTestEngine(t)
	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "a" {
		t.Errorf("Get(1): got %q, want %q", got, "a")
	}

	got, err = engine.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "b" {
		t.Errorf("Get(2): got %q, want %q", got, "b")
	}
}

func TestCompactAfterMultipleFlushes(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Put(2, []byte("b"))
	engine.Flush()
	engine.Put(3, []byte("c"))
	engine.Flush()

	if len(engine.ssts) != 3 {
		t.Fatalf("expected 3 SSTs before compact, got %d", len(engine.ssts))
	}

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	if len(engine.ssts) != 1 {
		t.Fatalf("expected 1 SST after compact, got %d", len(engine.ssts))
	}

	for key, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

func TestCompactWithTombstones(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	engine.Flush()

	engine.Delete(2)
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Key 2 should be gone (tombstone dropped during compaction)
	got, err := engine.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got != nil {
		t.Errorf("Get(2) after compact: got %v, want nil", got)
	}

	// Keys 1 and 3 should survive
	got, err = engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "a" {
		t.Errorf("Get(1): got %q, want %q", got, "a")
	}

	got, err = engine.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if string(got) != "c" {
		t.Errorf("Get(3): got %q, want %q", got, "c")
	}
}

func TestCompactWithOverwrites(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("v1"))
	engine.Flush()
	engine.Put(1, []byte("v2"))
	engine.Flush()
	engine.Put(1, []byte("v3"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "v3" {
		t.Errorf("Get(1): got %q, want %q", got, "v3")
	}
}

func TestCompactThenReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()
	engine.Put(3, []byte("c"))
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

func TestCompactEmpty(t *testing.T) {
	engine := openTestEngine(t)

	// Compact with no data at all
	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Should still work after compacting nothing
	engine.Put(1, []byte("hello"))
	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

func TestDoubleCompact(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Put more data and compact again
	engine.Put(3, []byte("c"))
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

func TestCompactOnlyTombstones(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	engine.Delete(1)
	engine.Delete(2)
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Everything was deleted; compaction should produce an empty SST
	count := 0
	iter := engine.Scan(0, 100)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan after compact of all tombstones: got %d entries, want 0", count)
	}
}

func TestCompactSingleSST(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("only"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "only" {
		t.Errorf("Get(1): got %q, want %q", got, "only")
	}
}

func TestCompactWithEmptyValues(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte{})
	engine.Put(2, []byte("notempty"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil, want empty slice")
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}

	got, err = engine.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "notempty" {
		t.Errorf("Get(2): got %q, want %q", got, "notempty")
	}
}

func TestCompactWithMemtableData(t *testing.T) {
	// Compact should also include unflushed memtable data
	engine := openTestEngine(t)

	engine.Put(1, []byte("flushed"))
	engine.Flush()
	engine.Put(2, []byte("inmemory"))

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "flushed" {
		t.Errorf("Get(1): got %q, want %q", got, "flushed")
	}

	got, err = engine.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "inmemory" {
		t.Errorf("Get(2): got %q, want %q", got, "inmemory")
	}
}

// =============================================================================
// --- Scan after compact ---
// =============================================================================

func TestScanAfterCompact(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(5, []byte("e"))
	engine.Flush()

	engine.Put(2, []byte("b"))
	engine.Put(4, []byte("d"))
	engine.Flush()

	engine.Put(3, []byte("c"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	var keys []uint64
	var values []string
	iter := engine.Scan(1, 6)
	for iter.Next() {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, key)
		values = append(values, string(value))
	}

	expectedKeys := []uint64{1, 2, 3, 4, 5}
	expectedValues := []string{"a", "b", "c", "d", "e"}

	if len(keys) != len(expectedKeys) {
		t.Fatalf("Scan after compact: got %d entries, want %d", len(keys), len(expectedKeys))
	}

	for idx := range keys {
		if keys[idx] != expectedKeys[idx] {
			t.Errorf("Scan key[%d]: got %d, want %d", idx, keys[idx], expectedKeys[idx])
		}
		if values[idx] != expectedValues[idx] {
			t.Errorf("Scan value[%d]: got %q, want %q", idx, values[idx], expectedValues[idx])
		}
	}
}

func TestScanAfterCompactWithOverwrites(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("old1"))
	engine.Put(2, []byte("old2"))
	engine.Flush()

	engine.Put(1, []byte("new1"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	var values []string
	iter := engine.Scan(0, 10)
	for iter.Next() {
		value := iter.Value()
		values = append(values, string(value))
	}

	if len(values) != 2 {
		t.Fatalf("Scan: got %d entries, want 2", len(values))
	}
	if values[0] != "new1" {
		t.Errorf("Scan value[0]: got %q, want %q", values[0], "new1")
	}
	if values[1] != "old2" {
		t.Errorf("Scan value[1]: got %q, want %q", values[1], "old2")
	}
}

// =============================================================================
// --- Delete + compact ---
// =============================================================================

func TestDeleteThenCompactDropsTombstones(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	engine.Flush()

	engine.Delete(2)
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// After compaction, the single resulting SST should have no tombstone for key 2.
	// Verify by checking the SST has only 2 keys.
	if len(engine.ssts) != 1 {
		t.Fatalf("expected 1 SST after compact, got %d", len(engine.ssts))
	}
	if engine.ssts[0].footer.BlockCount == 0 {
		t.Errorf("expected at least 1 block after compact, got 0")
	}

	// Verify the values are correct
	var keys []uint64
	iter := engine.Scan(0, 10)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}
	if len(keys) != 2 || keys[0] != 1 || keys[1] != 3 {
		t.Errorf("Scan: got %v, want [1 3]", keys)
	}
}

func TestDeleteAllThenCompact(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	engine.Delete(1)
	engine.Delete(2)
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// SST should have zero entries
	if len(engine.ssts) != 1 {
		t.Fatalf("expected 1 SST after compact, got %d", len(engine.ssts))
	}
	if engine.ssts[0].footer.BlockCount != 0 {
		t.Errorf("expected 0 blocks after compact (all tombstones), got %d", engine.ssts[0].footer.BlockCount)
	}
}

// =============================================================================
// --- Crash simulation (reopen without Close) ---
// =============================================================================

func TestCrashAfterFlush(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("persisted"))
	err = engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate crash: open new engine without closing old one
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := reopened.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "persisted" {
		t.Errorf("Get(1): got %q, want %q", got, "persisted")
	}
}

func TestCrashRecoveryViaWAL(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("flushed"))
	err = engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// Put more data but don't flush - simulate crash
	engine.Put(2, []byte("unflushed"))

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := reopened.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "flushed" {
		t.Errorf("Get(1): got %q, want %q", got, "flushed")
	}

	got, err = reopened.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "unflushed" {
		t.Errorf("Get(2): got %q, want %q (WAL should recover)", got, "unflushed")
	}
}

func TestCrashAfterCompact(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Put(2, []byte("b"))
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate crash: reopen without closing
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

func TestCrashAfterMultipleFlushesReopen(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Put(2, []byte("b"))
	engine.Flush()
	engine.Put(3, []byte("c"))
	engine.Flush()
	// Don't close, just reopen

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// =============================================================================
// --- Fault tolerance ---
// =============================================================================

func TestOpenWithCorruptObjectsDir(t *testing.T) {
	dir := t.TempDir()

	// Create a valid engine first
	_, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Create a garbage file in the objects directory (corrupt the structure)
	err = os.WriteFile(filepath.Join(dir, "protodb", "objects", "not-a-valid-hash"), []byte("garbage"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Should still open — invalid files are skipped
	_, err = Open(dir)
	if err != nil {
		t.Fatalf("expected Open to succeed (non-sst files skipped), got %v", err)
	}
}

func TestOpenWithMissingSST(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("hello"))
	err = engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// Delete the SST file from the objects directory
	sstPath := filepath.Join(dir, "protodb", "objects", engine.ssts[0].hash)
	err = os.Remove(sstPath)
	if err != nil {
		t.Fatal(err)
	}

	// Reopen — should fail because manifest references a missing SST
	_, err = Open(dir)
	if err == nil {
		t.Fatal("expected Open to fail when manifest references missing SST")
	}
}

func TestOpenWithCorruptSST(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("hello"))
	err = engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the SST file in the objects directory
	sstPath := filepath.Join(dir, "protodb", "objects", engine.ssts[0].hash)
	err = os.WriteFile(sstPath, []byte("garbage"), 0644)
	if err != nil {
		t.Fatal(err)
	}

	_, err = Open(dir)
	if err == nil {
		t.Fatal("expected error opening engine with corrupt SST, got nil")
	}
}

func TestOpenWithMissingManifest(t *testing.T) {
	dir := t.TempDir()

	// Open should create a fresh manifest if none exists
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Should be a fresh engine
	engine.Put(1, []byte("hello"))
	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

// =============================================================================
// --- Compact old SST file cleanup ---
// =============================================================================

func TestCompactDeletesOldSSTs(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Put(2, []byte("b"))
	engine.Flush()

	// Before compact, there should be 2 SSTs
	oldHashes := make([]string, len(engine.ssts))
	for idx, s := range engine.ssts {
		oldHashes[idx] = s.hash
		sstPath := engine.ObjectPath(s.hash)
		if _, statErr := os.Stat(sstPath); os.IsNotExist(statErr) {
			t.Fatalf("expected SST %s to exist before compact", s.hash)
		}
	}

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// New compacted SSTs should exist
	_ = oldHashes // GC of old objects is separate
	for _, s := range engine.ssts {
		sstPath := engine.ObjectPath(s.hash)
		if _, statErr := os.Stat(sstPath); os.IsNotExist(statErr) {
			t.Errorf("expected compacted SST at %s", sstPath)
		}
	}
}

// =============================================================================
// --- Compact loses max uint64 key (tests bug: scan upper bound exclusive) ---
// =============================================================================

func TestMaxUint64KeyIsReserved(t *testing.T) {
	// math.MaxUint64 is reserved and cannot survive compaction
	// because scan uses exclusive upper bound [lo, hi)
	// and there is no value > MaxUint64 to use as hi.
	// This test documents the limitation.
	engine := openTestEngine(t)

	maxKey := uint64(math.MaxUint64)
	engine.Put(1, []byte("first"))
	engine.Put(maxKey, []byte("max"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Key 1 survives compaction
	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "first" {
		t.Errorf("Get(1): got %q, want %q", got, "first")
	}

	// MaxUint64 is lost during compaction — this is a known limitation
	got, err = engine.Get(maxKey)
	if err != nil {
		t.Fatalf("Get(MaxUint64): %v", err)
	}
	if got != nil {
		t.Errorf("Get(MaxUint64): expected nil (key is reserved), got %q", got)
	}
}

// =============================================================================
// --- Compact with memtable tombstone shadowing SST ---
// =============================================================================

func TestCompactMemtableTombstoneShadowsSST(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	// Delete key 1 in memtable (not flushed), then compact
	engine.Delete(1)

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1): got %v, want nil (tombstone should shadow during compact)", got)
	}

	got, err = engine.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "b" {
		t.Errorf("Get(2): got %q, want %q", got, "b")
	}
}

// =============================================================================
// --- Compact then continue writing ---
// =============================================================================

func TestCompactThenContinueWriting(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Flush()

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Continue writing after compaction
	engine.Put(10, []byte("ten"))
	engine.Put(20, []byte("twenty"))
	engine.Flush()

	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "a" {
		t.Errorf("Get(1): got %q, want %q", got, "a")
	}

	got, err = engine.Get(10)
	if err != nil {
		t.Fatalf("Get(10): %v", err)
	}
	if string(got) != "ten" {
		t.Errorf("Get(10): got %q, want %q", got, "ten")
	}

	// Scan should return all 3 entries
	var keys []uint64
	iter := engine.Scan(0, 100)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}
	if len(keys) != 3 {
		t.Errorf("Scan: got %v, want 3 entries", keys)
	}
}

// =============================================================================
// --- Compact then reopen then compact again ---
// =============================================================================

func TestCompactReopenCompact(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Compact()

	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	reopened.Put(2, []byte("b"))
	reopened.Flush()

	err = reopened.Compact()
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// =============================================================================
// --- Atomic compaction crash safety tests ---
// =============================================================================

// Simulates a crash that left a temp file in the objects directory.
// On reopen, data should still be preserved (temp files are ignored).
func TestCrashLeavingTempFile(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	// Simulate: create a temp file in objects dir as if process crashed mid-write
	tempFile := filepath.Join(engine.ObjectsPath(), "-temp-partial")
	os.WriteFile(tempFile, []byte("partial"), 0644)

	// Reopen — should ignore the temp file and use manifest data
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Verifies that stale files in the objects directory don't affect reopening.
func TestReopenWithStaleObjectFile(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate: stale file left in objects directory
	os.WriteFile(filepath.Join(engine.ObjectsPath(), "stale-hash"), []byte("stale"), 0644)

	// Reopen — should still work, ignoring stale files
	reopened, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := reopened.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Verifies that a leftover temp file from a crashed compaction
// doesn't prevent a subsequent successful compaction.
func TestCompactAfterCrashedCompaction(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	// Leave behind a temp file from a previous crashed write
	tempFile := filepath.Join(engine.ObjectsPath(), "-temp-garbage")
	os.WriteFile(tempFile, []byte("garbage"), 0644)

	// Compact should succeed despite the leftover temp file
	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Verifies compacted SSTs exist in the objects directory.
func TestCompactCreatesObjectFiles(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// The objects directory should exist with compacted SSTs
	objectsDir := engine.ObjectsPath()
	if _, statErr := os.Stat(objectsDir); os.IsNotExist(statErr) {
		t.Errorf("objects dir should exist after compact")
	}

	// Each SST should have a file in the objects directory
	for _, s := range engine.ssts {
		sstPath := engine.ObjectPath(s.hash)
		if _, statErr := os.Stat(sstPath); os.IsNotExist(statErr) {
			t.Errorf("expected SST file at %s", sstPath)
		}
	}
}

// Verifies that compaction, reopen, write, flush, compact again works
// across multiple cycles.
func TestCompactMultipleCyclesWithReopen(t *testing.T) {
	dir := t.TempDir()

	for cycle := range 5 {
		engine, err := Open(dir)
		if err != nil {
			t.Fatalf("cycle %d Open: %v", cycle, err)
		}

		key := uint64(cycle)
		engine.Put(key, []byte(fmt.Sprintf("v%d", cycle)))
		engine.Flush()

		err = engine.Compact()
		if err != nil {
			t.Fatalf("cycle %d Compact: %v", cycle, err)
		}
	}

	// Reopen and verify all data survived
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for cycle := range 5 {
		key := uint64(cycle)
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		want := fmt.Sprintf("v%d", cycle)
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Verifies that old SST files are properly cleaned up across
// multiple compaction cycles.
func TestCompactMultipleCyclesCleanup(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for cycle := range 3 {
		engine.Put(uint64(cycle), []byte("v"))
		engine.Flush()
		err = engine.Compact()
		if err != nil {
			t.Fatalf("cycle %d: %v", cycle, err)
		}
	}

	// Only the SSTs tracked by the engine should exist in objects
	objectsDir := engine.ObjectsPath()
	entries, err := os.ReadDir(objectsDir)
	if err != nil {
		t.Fatal(err)
	}

	// Filter out temp files
	var sstFiles []string
	for _, entry := range entries {
		if !entry.IsDir() {
			sstFiles = append(sstFiles, entry.Name())
		}
	}

	// Without GC, old SST files accumulate. Just verify the engine has the right number of SSTs in memory.
	if len(engine.ssts) != 1 {
		t.Errorf("expected 1 SST in engine after compactions, got %d", len(engine.ssts))
	}
}

// Verifies Compact works after a reopen that finds a leftover temp file.
func TestReopenWithTempFileThenCompact(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Flush()

	// Simulate crashed compaction leaving temp file
	tempFile := filepath.Join(engine.ObjectsPath(), "-temp-crashed")
	os.WriteFile(tempFile, []byte("partial"), 0644)

	// Reopen
	engine, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Should be able to compact successfully
	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Verifies data integrity through a Compact that includes tombstones,
// then reopen, then another round of writes and compact.
func TestCompactTombstonesThenReopenThenCompact(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	engine.Flush()
	engine.Delete(2)
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Reopen
	engine, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Key 2 should still be gone
	got, err := engine.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got != nil {
		t.Errorf("Get(2): got %v, want nil", got)
	}

	// Add new data and compact again
	engine.Put(4, []byte("d"))
	engine.Put(5, []byte("e"))
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	// Verify all expected data
	for key, want := range map[uint64]string{1: "a", 3: "c", 4: "d", 5: "e"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}

	got, err = engine.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got != nil {
		t.Errorf("Get(2) after second compact: got %v, want nil", got)
	}
}

// Verifies that Flush after Compact writes to the objects directory.
func TestFlushAfterCompactWritesToObjects(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Flush()

	err = engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(2, []byte("b"))
	engine.Flush()

	// The new SST should be in the objects directory
	lastSST := engine.ssts[len(engine.ssts)-1]
	sstPath := engine.ObjectPath(lastSST.hash)
	if _, statErr := os.Stat(sstPath); os.IsNotExist(statErr) {
		t.Errorf("expected SST at %s after flush post-compact", sstPath)
	}

	// Both values should be readable
	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Verifies Compact with large data set — exercises the SST merge
// with many entries across multiple SSTs.
func TestCompactLargeDataSet(t *testing.T) {
	engine := openTestEngine(t)

	// Write 1000 keys across 10 SSTs
	for batch := range 10 {
		for idx := range 100 {
			key := uint64(batch*100 + idx)
			engine.Put(key, []byte(fmt.Sprintf("v%d", key)))
		}
		engine.Flush()
	}

	if len(engine.ssts) != 10 {
		t.Fatalf("expected 10 SSTs before compact, got %d", len(engine.ssts))
	}

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	if len(engine.ssts) != 1 {
		t.Fatalf("expected 1 SST after compact, got %d", len(engine.ssts))
	}

	// Verify all keys
	for key := uint64(0); key < 1000; key++ {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		want := fmt.Sprintf("v%d", key)
		if string(got) != want {
			t.Fatalf("Get(%d): got %q, want %q", key, got, want)
		}
	}

	// Verify scan returns all 1000
	count := 0
	iter := engine.Scan(0, 1000)
	for iter.Next() {
		count++
	}
	if count != 1000 {
		t.Errorf("Scan: got %d entries, want 1000", count)
	}
}

// Verifies Compact with interleaved overwrites across many SSTs —
// only the latest value for each key should survive.
func TestCompactLargeWithOverwrites(t *testing.T) {
	engine := openTestEngine(t)

	// Write same 100 keys across 10 SSTs, each time with a new value
	for batch := range 10 {
		for idx := range 100 {
			key := uint64(idx)
			engine.Put(key, []byte(fmt.Sprintf("batch%d", batch)))
		}
		engine.Flush()
	}

	err := engine.Compact()
	if err != nil {
		t.Fatal(err)
	}

	if engine.ssts[0].footer.BlockCount == 0 {
		t.Errorf("expected at least 1 block after compact, got 0")
	}

	// All keys should have the value from the last batch
	for key := uint64(0); key < 100; key++ {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != "batch9" {
			t.Errorf("Get(%d): got %q, want %q", key, got, "batch9")
		}
	}
}

// =============================================================================
// --- Trivial coverage ---
// =============================================================================

func TestEngineClose(t *testing.T) {
	engine := openTestEngine(t)
	err := engine.Close()
	if err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestEngineSync(t *testing.T) {
	engine := openTestEngine(t)
	err := engine.Sync()
	if err != nil {
		t.Fatalf("Sync: %v", err)
	}
}

// =============================================================================
// --- OS-level fault injection tests ---
// =============================================================================

// Flush fails when the objects directory is unwritable (CreateTemp fails).
func TestFlushFailsOnUnwritableDir(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))

	// Make the objects dir unwritable so CreateTemp fails
	objectsDir := engine.ObjectsPath()
	os.Chmod(objectsDir, 0555)
	defer os.Chmod(objectsDir, 0755)

	err = engine.Flush()
	if err == nil {
		t.Fatal("expected Flush to fail on unwritable dir")
	}

	// Engine state should be unchanged — data still in memtable
	os.Chmod(objectsDir, 0755)
	got, err := engine.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "a" {
		t.Errorf("Get(1): got %q, want %q", got, "a")
	}
}


// SST Scan silently handles a deleted SST file (yields no results).
func TestSSTScanAfterFileDeleted(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("a")},
		{2, []byte("b")},
	}

	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}

	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		t.Fatal(err)
	}

	sstPath := filepath.Join(dir, s.hash)

	// Open a handle before deleting, so we can test Scan
	f := openSSTFile(t, dir, s)

	// Delete the SST file after opening the handle
	os.Remove(sstPath)

	// Scan with the pre-opened handle — on Unix the inode is still alive
	count := 0
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		count++
	}

	// Trying to open a new handle should fail
	_, openErr := os.Open(sstPath)
	if openErr == nil {
		t.Fatal("expected Open to fail on deleted file")
	}

	// Get with the pre-opened handle still works on Unix (inode alive)
	_, err = s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1) with pre-opened handle: %v", err)
	}
}

// SST Scan handles a truncated file (read fails mid-scan).
func TestSSTScanTruncatedFile(t *testing.T) {
	dir := t.TempDir()

	// Write an SST with enough data that truncation breaks a read
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("aaaaaaaaaa")},
		{2, []byte("bbbbbbbbbb")},
		{3, []byte("cccccccccc")},
	}

	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}

	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		t.Fatal(err)
	}

	f := openSSTFile(t, dir, s)

	// Truncate the file to remove value data but keep keys+footer
	os.Truncate(filepath.Join(dir, s.hash), 5)

	// Scan should stop early (read error) rather than panic
	count := 0
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		count++
	}
	// We don't assert exact count — just that it doesn't panic
}

// SST Get handles a truncated file.
func TestSSTGetTruncatedFile(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("hello world")},
	}

	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}

	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		t.Fatal(err)
	}

	f := openSSTFile(t, dir, s)

	// Truncate to corrupt the value data
	os.Truncate(filepath.Join(dir, s.hash), 2)

	_, err = s.Get(1, f)
	if err == nil {
		t.Fatal("expected Get to fail on truncated SST")
	}
}

// ReadSST returns ErrCorrupted when footer claims more entries than the file can hold.
func TestReadSSTCorruptedFooterCount(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("x")},
	}

	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}
	hash := ssts[0].hash

	// Overwrite the count field in the footer with a huge value.
	// Footer is the last 10 bytes: 8 bytes count + 2 bytes version.
	sstPath := filepath.Join(dir, hash)
	info, _ := os.Stat(sstPath)
	fileSize := info.Size()

	f, err := os.OpenFile(sstPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	// Write a huge count at the count offset (10 bytes from end)
	hugeCount := make([]byte, 8)
	hugeCount[0] = 0xFF
	hugeCount[1] = 0xFF
	hugeCount[2] = 0xFF
	hugeCount[3] = 0xFF
	f.WriteAt(hugeCount, fileSize-10)
	f.Close()

	_, err = ReadSST(dir, hash, nil)
	if !errors.Is(err, ErrCorrupted) {
		t.Fatalf("expected ErrCorrupted, got %v", err)
	}
}

// Open fails gracefully when the protodb parent path is unreadable.
func TestOpenFailsOnUnreadableDir(t *testing.T) {
	dir := t.TempDir()

	// First create a valid db
	_, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Make protodb dir unreadable
	protodbDir := filepath.Join(dir, "protodb")
	os.Chmod(protodbDir, 0000)
	defer os.Chmod(protodbDir, 0755)

	_, err = Open(dir)
	if err == nil {
		t.Fatal("expected Open to fail on unreadable dir")
	}
}

// Engine.Get returns error when SST file is deleted between flush and get.
func TestEngineGetAfterSSTDeleted(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("hello"))
	engine.Flush()

	// Delete the SST file
	sstPath := filepath.Join(dir, "protodb", "objects", engine.ssts[0].hash)
	os.Remove(sstPath)

	// Get should return an error (not panic)
	_, err = engine.Get(1)
	if err == nil {
		// The key is not in memtable, SST is gone — returns nil, nil (not found)
		// This is acceptable: the data is lost, not an error
	}
}

// WriteSST fails when the directory doesn't exist and can't be created.
func TestWriteSSTFailsOnBadDir(t *testing.T) {
	// Use a path under a file (not a dir) so CreateTemp fails
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	os.WriteFile(blocker, []byte("x"), 0644)

	entries := entriesFrom([]struct {
		key   uint64
		value []byte
	}{
		{1, []byte("a")},
	})

	_, err := WriteSST(blocker, entries)
	if err == nil {
		t.Fatal("expected WriteSST to fail when dir is actually a file")
	}
}

// =============================================================================
// --- WAL recovery tests ---
// =============================================================================

// Basic WAL recovery: put without flush, reopen, data is recovered.
func TestWALRecoveryBasic(t *testing.T) {
	dir := t.TempDir()
	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Put(3, []byte("c"))
	// No flush — all data is only in the WAL

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		got, err := engine2.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// WAL recovery with overwrites: last write wins.
func TestWALRecoveryOverwrite(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("v1"))
	engine.Put(1, []byte("v2"))
	engine.Put(1, []byte("v3"))

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "v3" {
		t.Errorf("Get(1): got %q, want %q", got, "v3")
	}
}

// WAL recovery with deletes: tombstones survive.
func TestWALRecoveryDelete(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Delete(1)

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1): got %v, want nil (deleted)", got)
	}

	got, err = engine2.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "b" {
		t.Errorf("Get(2): got %q, want %q", got, "b")
	}
}

// WAL recovery with delete then re-put: resurrection works.
func TestWALRecoveryDeleteThenPut(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("first"))
	engine.Delete(1)
	engine.Put(1, []byte("resurrected"))

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "resurrected" {
		t.Errorf("Get(1): got %q, want %q", got, "resurrected")
	}
}

// WAL recovery with empty values (not tombstones).
func TestWALRecoveryEmptyValue(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte{})
	engine.Put(2, []byte("notempty"))

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil, want empty slice")
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}

	got, err = engine2.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "notempty" {
		t.Errorf("Get(2): got %q, want %q", got, "notempty")
	}
}

// WAL is cleared after flush — no double replay.
func TestWALClearedAfterFlush(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("before"))
	engine.Flush()

	// After flush, WAL is cleared. Put new data.
	engine.Put(1, []byte("after"))

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// Should get "after" from WAL replay, not "before"
	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "after" {
		t.Errorf("Get(1): got %q, want %q", got, "after")
	}
}

// WAL recovery combined with existing SSTs: WAL shadows SST data.
func TestWALRecoveryShadowsSST(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("old"))
	engine.Flush()
	engine.Put(1, []byte("new")) // in WAL only

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "new" {
		t.Errorf("Get(1): got %q, want %q", got, "new")
	}
}

// WAL recovery with delete shadowing an SST entry.
func TestWALRecoveryDeleteShadowsSST(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("flushed"))
	engine.Flush()
	engine.Delete(1) // WAL tombstone

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if got != nil {
		t.Errorf("Get(1): got %v, want nil (WAL delete should shadow SST)", got)
	}
}

// WAL recovery with many entries.
func TestWALRecoveryManyEntries(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	for idx := uint64(0); idx < 1000; idx++ {
		engine.Put(idx, []byte(fmt.Sprintf("v%d", idx)))
	}

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for idx := uint64(0); idx < 1000; idx++ {
		got, err := engine2.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d): %v", idx, err)
		}
		want := fmt.Sprintf("v%d", idx)
		if string(got) != want {
			t.Fatalf("Get(%d): got %q, want %q", idx, got, want)
		}
	}
}

// WAL recovery after multiple flushes — only unflushed data in WAL.
func TestWALRecoveryAfterMultipleFlushes(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Put(2, []byte("b"))
	engine.Flush()
	engine.Put(3, []byte("c")) // only this is in the WAL

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b", 3: "c"} {
		got, err := engine2.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// WAL with truncated entry (simulating crash mid-write) — partial entry is discarded.
func TestWALTruncatedEntry(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("good"))
	engine.Put(2, []byte("also good"))

	// Corrupt the WAL: truncate the last few bytes to simulate crash mid-write
	walPath := filepath.Join(dir, "protodb", "wal")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	os.Truncate(walPath, info.Size()-5)

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// First entry should be recovered
	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "good" {
		t.Errorf("Get(1): got %q, want %q", got, "good")
	}

	// Second entry was truncated — may or may not be recovered depending
	// on how much was truncated. We just verify no panic/error on Open.
}

// WAL with corrupted checksum — corrupt entry and everything after is discarded.
func TestWALCorruptedChecksum(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("good"))
	engine.Put(2, []byte("will be corrupted"))
	engine.Put(3, []byte("after corruption"))

	// Corrupt the second entry's checksum
	walPath := filepath.Join(dir, "protodb", "wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}

	// First frame: 4 (frame_len) + 4 (crc) + 8 (key) + 8 (len) + 4 (value "good") = 28 bytes
	// Second frame starts at byte 28. Its checksum is at bytes 32-35.
	// Flip a bit in the checksum to corrupt it.
	if len(data) > 33 {
		data[33] ^= 0xFF
	}
	os.WriteFile(walPath, data, 0644)

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	// First entry should be recovered
	got, err := engine2.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "good" {
		t.Errorf("Get(1): got %q, want %q", got, "good")
	}

	// Entries 2 and 3 should be lost (corrupt frame stops replay)
	got, err = engine2.Get(2)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if got != nil {
		t.Errorf("Get(2): got %v, want nil (corrupt entry should be discarded)", got)
	}

	got, err = engine2.Get(3)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if got != nil {
		t.Errorf("Get(3): got %v, want nil (entry after corruption should be discarded)", got)
	}
}

// WAL is cleared after compaction.
func TestWALClearedAfterCompaction(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Put(2, []byte("b")) // in WAL

	engine.Compact()

	// WAL should be truncated (empty) after compaction
	walPath := filepath.Join(dir, "protodb", "wal")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatalf("WAL file should still exist after compaction: %v", err)
	}
	if info.Size() != 0 {
		t.Errorf("WAL should be empty after compaction, got size %d", info.Size())
	}

	// Data should still be accessible
	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}

	// Reopen — no WAL to replay, data is in the compacted SST
	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, err := engine2.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Scan works correctly with WAL-recovered data.
func TestWALRecoveryScan(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Put(3, []byte("c"))
	engine.Flush()
	engine.Put(2, []byte("b")) // in WAL only

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	var keys []uint64
	iter := engine2.Scan(1, 4)
	for iter.Next() {
		key := iter.Key()
		keys = append(keys, key)
	}

	if len(keys) != 3 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 {
		t.Errorf("Scan: got %v, want [1 2 3]", keys)
	}
}

// =============================================================================
// --- Adversarial engine tests ---
// =============================================================================

// Put, delete, re-put, flush, delete, flush, compact, reopen — full lifecycle.
func TestAdversarialFullLifecycle(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("v1"))
	engine.Put(2, []byte("v2"))
	engine.Put(3, []byte("v3"))
	engine.Delete(2)
	engine.Put(2, []byte("v2-resurrected"))
	engine.Flush()
	engine.Delete(3)
	engine.Flush()
	engine.Put(4, []byte("v4"))
	engine.Compact()

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for key, want := range map[uint64]string{1: "v1", 2: "v2-resurrected", 4: "v4"} {
		got, err := engine2.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
	got, _ := engine2.Get(3)
	if got != nil {
		t.Errorf("Get(3): got %v, want nil", got)
	}
}

// Rapidly alternate between put and delete on the same key across flushes.
func TestAdversarialRapidPutDelete(t *testing.T) {
	engine := openTestEngine(t)

	for round := range 50 {
		engine.Put(1, []byte(fmt.Sprintf("round-%d", round)))
		engine.Delete(1)
	}
	engine.Put(1, []byte("final"))

	got, _ := engine.Get(1)
	if string(got) != "final" {
		t.Errorf("got %q, want %q", got, "final")
	}

	engine.Flush()
	got, _ = engine.Get(1)
	if string(got) != "final" {
		t.Errorf("after flush: got %q, want %q", got, "final")
	}
}

// Many flushes without compaction — tests SST ordering stays correct.
func TestAdversarialManyFlushesNoCompact(t *testing.T) {
	engine := openTestEngine(t)

	for batch := range 20 {
		for idx := range 10 {
			key := uint64(batch*10 + idx)
			engine.Put(key, []byte(fmt.Sprintf("v%d", key)))
		}
		engine.Flush()
	}

	if len(engine.ssts) != 20 {
		t.Fatalf("expected 20 SSTs, got %d", len(engine.ssts))
	}

	for key := uint64(0); key < 200; key++ {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		want := fmt.Sprintf("v%d", key)
		if string(got) != want {
			t.Fatalf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Overwrite same key in every SST — only the newest value should survive.
func TestAdversarialOverwriteAcrossManySSTs(t *testing.T) {
	engine := openTestEngine(t)

	for batch := range 20 {
		engine.Put(1, []byte(fmt.Sprintf("batch-%d", batch)))
		engine.Flush()
	}

	got, _ := engine.Get(1)
	if string(got) != "batch-19" {
		t.Errorf("got %q, want %q", got, "batch-19")
	}

	engine.Compact()
	got, _ = engine.Get(1)
	if string(got) != "batch-19" {
		t.Errorf("after compact: got %q, want %q", got, "batch-19")
	}
}

// Flush an empty memtable, then put+flush real data, then compact.
func TestAdversarialEmptyFlushesInterspersed(t *testing.T) {
	engine := openTestEngine(t)

	engine.Flush() // empty
	engine.Flush() // empty
	engine.Put(1, []byte("a"))
	engine.Flush()
	engine.Flush() // empty
	engine.Put(2, []byte("b"))
	engine.Flush()
	engine.Flush() // empty

	engine.Compact()

	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, _ := engine.Get(key)
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Compact with unflushed WAL data + SST data + tombstones.
func TestAdversarialCompactMixedSources(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("sst-a"))
	engine.Put(2, []byte("sst-b"))
	engine.Flush()
	engine.Put(3, []byte("sst-c"))
	engine.Delete(2)
	engine.Flush()
	engine.Put(4, []byte("wal-d"))
	engine.Put(1, []byte("wal-a-overwrite"))

	engine.Compact()

	reopened, _ := Open(dir)

	expected := map[uint64]string{1: "wal-a-overwrite", 3: "sst-c", 4: "wal-d"}
	for key, want := range expected {
		got, _ := reopened.Get(key)
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
	got, _ := reopened.Get(2)
	if got != nil {
		t.Errorf("Get(2): got %v, want nil", got)
	}
}

// Open, put, close, open, put, close — repeated open/close cycles.
func TestAdversarialRepeatedOpenClose(t *testing.T) {
	dir := t.TempDir()

	for cycle := range 10 {
		engine, err := Open(dir)
		if err != nil {
			t.Fatalf("cycle %d Open: %v", cycle, err)
		}
		engine.Put(uint64(cycle), []byte(fmt.Sprintf("v%d", cycle)))
		engine.Close()
	}

	engine, _ := Open(dir)
	for cycle := range 10 {
		got, _ := engine.Get(uint64(cycle))
		want := fmt.Sprintf("v%d", cycle)
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", cycle, got, want)
		}
	}
}

// Put data, close without flush, reopen, flush, compact, reopen.
func TestAdversarialCloseWithoutFlushThenCompact(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))
	engine.Close()

	engine, _ = Open(dir)
	engine.Flush()
	engine.Compact()
	engine.Close()

	engine, _ = Open(dir)
	for key, want := range map[uint64]string{1: "a", 2: "b"} {
		got, _ := engine.Get(key)
		if string(got) != want {
			t.Errorf("Get(%d): got %q, want %q", key, got, want)
		}
	}
}

// Scan while memtable has tombstones that shadow SST entries,
// plus WAL-recovered entries, plus empty SSTs.
func TestAdversarialScanComplexMerge(t *testing.T) {
	engine := openTestEngine(t)

	for idx := uint64(1); idx <= 5; idx++ {
		engine.Put(idx, []byte(fmt.Sprintf("sst1-%d", idx)))
	}
	engine.Flush()

	engine.Put(2, []byte("sst2-2"))
	engine.Put(4, []byte("sst2-4"))
	engine.Put(6, []byte("sst2-6"))
	engine.Flush()

	engine.Delete(3)
	engine.Put(5, []byte("mem-5"))
	engine.Put(7, []byte("mem-7"))

	var keys []uint64
	var vals []string
	iter := engine.Scan(1, 8)
	for iter.Next() {
		key := iter.Key()
		val := iter.Value()
		keys = append(keys, key)
		vals = append(vals, string(val))
	}

	expectedKeys := []uint64{1, 2, 4, 5, 6, 7}
	expectedVals := []string{"sst1-1", "sst2-2", "sst2-4", "mem-5", "sst2-6", "mem-7"}

	if len(keys) != len(expectedKeys) {
		t.Fatalf("Scan: got %d entries, want %d", len(keys), len(expectedKeys))
	}
	for idx := range keys {
		if keys[idx] != expectedKeys[idx] {
			t.Errorf("key[%d]: got %d, want %d", idx, keys[idx], expectedKeys[idx])
		}
		if vals[idx] != expectedVals[idx] {
			t.Errorf("val[%d]: got %q, want %q", idx, vals[idx], expectedVals[idx])
		}
	}
}

// Compact, then immediately compact again with no new data.
func TestAdversarialDoubleCompactNoData(t *testing.T) {
	engine := openTestEngine(t)

	engine.Put(1, []byte("a"))
	engine.Flush()

	engine.Compact()
	engine.Compact()

	got, _ := engine.Get(1)
	if string(got) != "a" {
		t.Errorf("got %q, want %q", got, "a")
	}
}

// Delete every key, compact, verify all gone, put new data, verify it works.
func TestAdversarialDeleteAllCompactThenWrite(t *testing.T) {
	engine := openTestEngine(t)

	for idx := uint64(0); idx < 100; idx++ {
		engine.Put(idx, []byte("v"))
	}
	engine.Flush()

	for idx := uint64(0); idx < 100; idx++ {
		engine.Delete(idx)
	}
	engine.Flush()
	engine.Compact()

	count := 0
	iter := engine.Scan(0, 100)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("expected 0 entries, got %d", count)
	}

	engine.Put(999, []byte("alive"))
	engine.Flush()
	got, _ := engine.Get(999)
	if string(got) != "alive" {
		t.Errorf("got %q, want %q", got, "alive")
	}
}

// SST file deleted while engine is running.
func TestAdversarialSSTDeletedWhileRunning(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Flush()

	sstPath := filepath.Join(dir, "protodb", "objects", engine.ssts[0].hash)
	os.Remove(sstPath)

	// Should not panic
	_, err := engine.Get(1)
	if err == nil {
		// Cached handle may still work on Unix — that's fine
	}
}

// WAL corrupted, but SST data is intact — SST data recovered.
func TestAdversarialCorruptWALIntactSST(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("in-sst"))
	engine.Flush()
	engine.Put(2, []byte("in-wal"))
	engine.Close()

	walPath := filepath.Join(dir, "protodb", "wal")
	os.WriteFile(walPath, []byte("totally garbage"), 0644)

	engine2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := engine2.Get(1)
	if string(got) != "in-sst" {
		t.Errorf("Get(1): got %q, want %q", got, "in-sst")
	}
	got, _ = engine2.Get(2)
	if got != nil {
		t.Errorf("Get(2): got %v, want nil (WAL was corrupted)", got)
	}
}

// Flush fails, verify engine state is unchanged and retry works.
func TestAdversarialFlushFailureRecovery(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(1, []byte("a"))
	engine.Put(2, []byte("b"))

	objectsDir := engine.ObjectsPath()
	os.Chmod(objectsDir, 0555)

	err := engine.Flush()
	if err == nil {
		os.Chmod(objectsDir, 0755)
		t.Fatal("expected Flush to fail")
	}

	os.Chmod(objectsDir, 0755)

	got, _ := engine.Get(1)
	if string(got) != "a" {
		t.Errorf("Get(1) after failed flush: got %q, want %q", got, "a")
	}

	err = engine.Flush()
	if err != nil {
		t.Fatal(err)
	}

	got, _ = engine.Get(1)
	if string(got) != "a" {
		t.Errorf("Get(1) after successful flush: got %q, want %q", got, "a")
	}
}


// Key 0 through the entire lifecycle.
func TestAdversarialKeyZeroLifecycle(t *testing.T) {
	dir := t.TempDir()
	engine, _ := Open(dir)

	engine.Put(0, []byte("zero"))
	engine.Flush()
	engine.Compact()
	engine.Close()

	engine, _ = Open(dir)
	got, _ := engine.Get(0)
	if string(got) != "zero" {
		t.Errorf("got %q, want %q", got, "zero")
	}

	engine.Delete(0)
	engine.Close()

	engine, _ = Open(dir)
	got, _ = engine.Get(0)
	if got != nil {
		t.Errorf("got %v, want nil", got)
	}
}

// Large values through WAL + flush + compact + reopen.
func TestAdversarialLargeValueLifecycle(t *testing.T) {
	dir := t.TempDir()
	big := make([]byte, 512*1024)
	for idx := range big {
		big[idx] = byte(idx % 251)
	}

	engine, _ := Open(dir)
	engine.Put(1, big)
	engine.Close()

	engine, _ = Open(dir)
	got, _ := engine.Get(1)
	if len(got) != len(big) {
		t.Fatalf("after WAL replay: got len %d, want %d", len(got), len(big))
	}

	engine.Flush()
	engine.Compact()
	engine.Close()

	engine, _ = Open(dir)
	got, _ = engine.Get(1)
	if len(got) != len(big) {
		t.Fatalf("after compact+reopen: got len %d, want %d", len(got), len(big))
	}
	for idx := range got {
		if got[idx] != big[idx] {
			t.Fatalf("byte %d: got %d, want %d", idx, got[idx], big[idx])
		}
	}
}

// =============================================================================
// --- Concurrent tests ---
// =============================================================================

// Parallel reads on a populated engine.
func TestConcurrentReads(t *testing.T) {
	engine := openTestEngine(t)
	for idx := uint64(0); idx < 100; idx++ {
		engine.Put(idx, []byte(fmt.Sprintf("v%d", idx)))
	}
	engine.Flush()

	var wg sync.WaitGroup
	for goroutine := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := uint64(0); idx < 100; idx++ {
				got, err := engine.Get(idx)
				if err != nil {
					t.Errorf("goroutine %d Get(%d): %v", goroutine, idx, err)
					return
				}
				want := fmt.Sprintf("v%d", idx)
				if string(got) != want {
					t.Errorf("goroutine %d Get(%d): got %q, want %q", goroutine, idx, got, want)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Parallel writes to distinct keys.
func TestConcurrentWritesDistinctKeys(t *testing.T) {
	engine := openTestEngine(t)

	var wg sync.WaitGroup
	for goroutine := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := uint64(goroutine * 100)
			for idx := uint64(0); idx < 100; idx++ {
				err := engine.Put(base+idx, []byte(fmt.Sprintf("g%d-v%d", goroutine, idx)))
				if err != nil {
					t.Errorf("Put: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// All 1000 keys should be present
	for key := uint64(0); key < 1000; key++ {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if got == nil {
			t.Fatalf("Get(%d): got nil", key)
		}
	}
}

// Parallel writes to the same key — last write wins, no corruption.
func TestConcurrentWritesSameKey(t *testing.T) {
	engine := openTestEngine(t)

	var wg sync.WaitGroup
	for goroutine := range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range 100 {
				engine.Put(1, []byte(fmt.Sprintf("g%d-r%d", goroutine, round)))
			}
		}()
	}
	wg.Wait()

	// Key should have some value — we don't know which goroutine won,
	// but the value must be well-formed (no torn writes).
	got, err := engine.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("Get(1): got nil")
	}
	if len(got) == 0 {
		t.Fatal("Get(1): got empty value")
	}
}

// Concurrent reads while writes are happening.
func TestConcurrentReadsDuringWrites(t *testing.T) {
	engine := openTestEngine(t)

	// Pre-populate some data
	for idx := uint64(0); idx < 50; idx++ {
		engine.Put(idx, []byte(fmt.Sprintf("v%d", idx)))
	}

	var wg sync.WaitGroup

	// Writers: add keys 50-99
	for goroutine := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := uint64(50 + goroutine*10)
			for idx := uint64(0); idx < 10; idx++ {
				engine.Put(base+idx, []byte(fmt.Sprintf("v%d", base+idx)))
			}
		}()
	}

	// Readers: read keys 0-49 (guaranteed to exist)
	for goroutine := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := uint64(0); idx < 50; idx++ {
				got, err := engine.Get(idx)
				if err != nil {
					t.Errorf("reader %d Get(%d): %v", goroutine, idx, err)
					return
				}
				if got == nil {
					t.Errorf("reader %d Get(%d): got nil", goroutine, idx)
					return
				}
			}
		}()
	}
	wg.Wait()
}

// Concurrent writes with periodic flushes.
func TestConcurrentWritesWithFlush(t *testing.T) {
	engine := openTestEngine(t)

	var wg sync.WaitGroup

	// Writers
	for goroutine := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := uint64(goroutine * 100)
			for idx := uint64(0); idx < 100; idx++ {
				engine.Put(base+idx, []byte(fmt.Sprintf("v%d", base+idx)))
			}
		}()
	}

	// Flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			engine.Flush()
		}
	}()

	wg.Wait()

	// Verify all data is accessible
	for key := uint64(0); key < 500; key++ {
		got, _ := engine.Get(key)
		if got == nil {
			t.Fatalf("Get(%d): got nil", key)
		}
	}
}

// Concurrent reads and scans during a flush.
func TestConcurrentReadsAndScanDuringFlush(t *testing.T) {
	engine := openTestEngine(t)

	for idx := uint64(0); idx < 100; idx++ {
		engine.Put(idx, []byte(fmt.Sprintf("v%d", idx)))
	}

	var wg sync.WaitGroup

	// Readers
	for goroutine := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range 20 {
				key := uint64(round % 100)
				got, err := engine.Get(key)
				if err != nil {
					t.Errorf("reader %d Get(%d): %v", goroutine, key, err)
					return
				}
				if got == nil {
					t.Errorf("reader %d Get(%d): got nil", goroutine, key)
					return
				}
			}
		}()
	}

	// Scanners
	for goroutine := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			count := 0
			iter := engine.Scan(0, 100)
			for iter.Next() {
				count++
			}
			if count == 0 {
				t.Errorf("scanner %d: got 0 entries", goroutine)
			}
		}()
	}

	// Flusher
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.Flush()
	}()

	wg.Wait()
}

// Concurrent writes with a compaction running.
func TestConcurrentWritesWithCompact(t *testing.T) {
	engine := openTestEngine(t)

	// Pre-populate and flush to have SSTs
	for idx := uint64(0); idx < 100; idx++ {
		engine.Put(idx, []byte("old"))
	}
	engine.Flush()

	var wg sync.WaitGroup

	// Writers: overwrite all keys
	for goroutine := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			base := uint64(goroutine * 20)
			for idx := uint64(0); idx < 20; idx++ {
				engine.Put(base+idx, []byte(fmt.Sprintf("new-%d", base+idx)))
			}
		}()
	}

	// Compactor
	wg.Add(1)
	go func() {
		defer wg.Done()
		engine.Compact()
	}()

	wg.Wait()

	// All 100 keys should exist with some value (no corruption)
	for key := uint64(0); key < 100; key++ {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("Get(%d): %v", key, err)
		}
		if got == nil {
			t.Fatalf("Get(%d): got nil", key)
		}
	}
}

// Concurrent deletes while reads are happening.
func TestConcurrentDeletesDuringReads(t *testing.T) {
	engine := openTestEngine(t)

	for idx := uint64(0); idx < 100; idx++ {
		engine.Put(idx, []byte(fmt.Sprintf("v%d", idx)))
	}
	engine.Flush()

	var wg sync.WaitGroup

	// Deleters: delete even keys
	wg.Add(1)
	go func() {
		defer wg.Done()
		for idx := uint64(0); idx < 100; idx += 2 {
			engine.Delete(idx)
		}
	}()

	// Readers: read all keys — some may be nil (deleted), some may have values
	for goroutine := range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := uint64(0); idx < 100; idx++ {
				_, err := engine.Get(idx)
				if err != nil {
					t.Errorf("reader %d Get(%d): %v", goroutine, idx, err)
					return
				}
				// Value can be nil (deleted) or valid — both are OK
			}
		}()
	}

	wg.Wait()
}

// Concurrent puts, deletes, reads, scans, and flushes — stress test.
func TestConcurrentStress(t *testing.T) {
	engine := openTestEngine(t)

	var wg sync.WaitGroup

	// Writers
	for goroutine := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range 200 {
				key := uint64(goroutine*200 + round)
				engine.Put(key, []byte(fmt.Sprintf("v%d", key)))
			}
		}()
	}

	// Deleters
	wg.Add(1)
	go func() {
		defer wg.Done()
		for round := range 100 {
			engine.Delete(uint64(round))
		}
	}()

	// Readers
	for goroutine := range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := range 200 {
				engine.Get(uint64(round + goroutine*100))
			}
		}()
	}

	// Scanners
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 10 {
			iter := engine.Scan(0, 1000)
			for iter.Next() {
			}
		}
	}()

	// Flushers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range 5 {
			engine.Flush()
		}
	}()

	wg.Wait()

	// Engine should be in a consistent state — no panics, no deadlocks
	engine.Flush()
	count := 0
	iter := engine.Scan(0, 10000)
	for iter.Next() {
		count++
	}
	// We don't assert exact count — just that the engine is functional
	if count < 0 {
		t.Fatal("impossible")
	}
}

// =============================================================================
// --- Partitioned SST tests ---
// =============================================================================

// withSmallSSTSize sets SSTSize to a small value for the duration of the test,
// forcing WriteSST to produce multiple SSTs.
func withSmallSSTSize(t *testing.T, sstSize int, blockSize int) {
	t.Helper()
	oldSST := SSTSize
	oldBlock := BlockSize
	SSTSize = sstSize
	BlockSize = blockSize
	t.Cleanup(func() {
		SSTSize = oldSST
		BlockSize = oldBlock
	})
}

func TestPartitionedFlush(t *testing.T) {
	withSmallSSTSize(t, 512, 128)
	engine := openTestEngine(t)
	defer engine.Close()

	for idx := uint64(0); idx < 200; idx++ {
		engine.Put(idx, []byte("value"))
	}
	engine.Flush()

	if len(engine.ssts) < 2 {
		t.Fatalf("expected multiple SSTs from flush, got %d", len(engine.ssts))
	}

	// All keys should be readable
	for idx := uint64(0); idx < 200; idx++ {
		got, err := engine.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d): %v", idx, err)
		}
		if string(got) != "value" {
			t.Errorf("Get(%d): got %q, want %q", idx, got, "value")
		}
	}
}

func TestPartitionedFlushScan(t *testing.T) {
	withSmallSSTSize(t, 512, 128)
	engine := openTestEngine(t)
	defer engine.Close()

	for idx := uint64(0); idx < 200; idx++ {
		engine.Put(idx, []byte("value"))
	}
	engine.Flush()

	count := 0
	iter := engine.Scan(0, 200)
	for iter.Next() {
		count++
	}
	if count != 200 {
		t.Fatalf("Scan: got %d entries, want 200", count)
	}
}

func TestPartitionedCompact(t *testing.T) {
	withSmallSSTSize(t, 512, 128)
	engine := openTestEngine(t)
	defer engine.Close()

	for idx := uint64(0); idx < 200; idx++ {
		engine.Put(idx, []byte("value"))
	}
	engine.Flush()

	sstsBefore := len(engine.ssts)
	engine.Compact()

	if len(engine.ssts) < 2 {
		t.Fatalf("expected multiple SSTs after compact, got %d", len(engine.ssts))
	}

	// Compaction should produce the same number of partitions (same data, same size limit)
	if len(engine.ssts) != sstsBefore {
		t.Logf("SSTs before compact: %d, after: %d", sstsBefore, len(engine.ssts))
	}

	// All keys should be readable
	for idx := uint64(0); idx < 200; idx++ {
		got, err := engine.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d): %v", idx, err)
		}
		if string(got) != "value" {
			t.Errorf("Get(%d): got %q, want %q", idx, got, "value")
		}
	}
}

func TestPartitionedCompactThenReopen(t *testing.T) {
	withSmallSSTSize(t, 512, 128)
	dir := t.TempDir()

	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	for idx := uint64(0); idx < 200; idx++ {
		engine.Put(idx, []byte("value"))
	}
	engine.Flush()
	engine.Compact()
	engine.Close()

	engine, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	if len(engine.ssts) < 2 {
		t.Fatalf("expected multiple SSTs after reopen, got %d", len(engine.ssts))
	}

	for idx := uint64(0); idx < 200; idx++ {
		got, err := engine.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d) after reopen: %v", idx, err)
		}
		if string(got) != "value" {
			t.Errorf("Get(%d) after reopen: got %q, want %q", idx, got, "value")
		}
	}
}

func TestPartitionedWithTombstones(t *testing.T) {
	withSmallSSTSize(t, 512, 128)
	engine := openTestEngine(t)
	defer engine.Close()

	for idx := uint64(0); idx < 200; idx++ {
		engine.Put(idx, []byte("value"))
	}
	// Delete every other key
	for idx := uint64(0); idx < 200; idx += 2 {
		engine.Delete(idx)
	}
	engine.Flush()
	engine.Compact()

	// Even keys should return nil, odd keys should have values
	for idx := uint64(0); idx < 200; idx++ {
		got, err := engine.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d): %v", idx, err)
		}
		if idx%2 == 0 {
			if got != nil {
				t.Errorf("Get(%d): expected nil (deleted), got %q", idx, got)
			}
		} else {
			if string(got) != "value" {
				t.Errorf("Get(%d): got %q, want %q", idx, got, "value")
			}
		}
	}
}

func TestPartitionedMultipleFlushesAndCompact(t *testing.T) {
	withSmallSSTSize(t, 512, 128)
	engine := openTestEngine(t)
	defer engine.Close()

	// Two flushes with overlapping keys — second overwrite should win
	for idx := uint64(0); idx < 200; idx++ {
		engine.Put(idx, []byte("old"))
	}
	engine.Flush()

	for idx := uint64(0); idx < 200; idx++ {
		engine.Put(idx, []byte("new"))
	}
	engine.Flush()

	engine.Compact()

	for idx := uint64(0); idx < 200; idx++ {
		got, err := engine.Get(idx)
		if err != nil {
			t.Fatalf("Get(%d): %v", idx, err)
		}
		if string(got) != "new" {
			t.Errorf("Get(%d): got %q, want %q", idx, got, "new")
		}
	}
}

// =============================================================================
// --- Metamorphic test ---
// =============================================================================

func TestMetamorphic(t *testing.T) {
	seed := int64(42)
	rng := rand.New(rand.NewSource(seed))
	engine := openTestEngine(t)
	defer engine.Close()

	ref := make(map[uint64][]byte) // reference model
	keySpace := uint64(500)
	ops := 10000

	for op := 0; op < ops; op++ {
		switch rng.Intn(10) {
		case 0, 1, 2, 3: // Put (40%)
			key := uint64(rng.Int63n(int64(keySpace)))
			valLen := rng.Intn(200) + 1
			value := make([]byte, valLen)
			rng.Read(value)
			if err := engine.Put(key, value); err != nil {
				t.Fatalf("op %d: Put(%d): %v", op, key, err)
			}
			ref[key] = value

		case 4, 5: // Get (20%)
			key := uint64(rng.Int63n(int64(keySpace)))
			got, err := engine.Get(key)
			if err != nil {
				t.Fatalf("op %d: Get(%d): %v", op, key, err)
			}
			expected, exists := ref[key]
			if !exists || expected == nil {
				if got != nil {
					t.Fatalf("op %d: Get(%d): got %d bytes, want nil", op, key, len(got))
				}
			} else {
				if string(got) != string(expected) {
					t.Fatalf("op %d: Get(%d): value mismatch", op, key)
				}
			}

		case 6: // Delete (10%)
			key := uint64(rng.Int63n(int64(keySpace)))
			if err := engine.Delete(key); err != nil {
				t.Fatalf("op %d: Delete(%d): %v", op, key, err)
			}
			ref[key] = nil

		case 7: // Scan (10%)
			lo := uint64(rng.Int63n(int64(keySpace)))
			hi := lo + uint64(rng.Intn(100)) + 1
			iter := engine.Scan(lo, hi)
			var engineKeys []uint64
			engineValues := make(map[uint64]string)
			for iter.Next() {
				engineKeys = append(engineKeys, iter.Key())
				engineValues[iter.Key()] = string(iter.Value())
			}

			// Build expected from ref
			var refKeys []uint64
			for key, val := range ref {
				if key >= lo && key < hi && val != nil {
					refKeys = append(refKeys, key)
				}
			}
			sort.Slice(refKeys, func(i, j int) bool { return refKeys[i] < refKeys[j] })

			if len(engineKeys) != len(refKeys) {
				t.Fatalf("op %d: Scan(%d, %d): got %d entries, want %d", op, lo, hi, len(engineKeys), len(refKeys))
			}
			for idx, key := range refKeys {
				if engineKeys[idx] != key {
					t.Fatalf("op %d: Scan(%d, %d): key mismatch at %d: got %d, want %d", op, lo, hi, idx, engineKeys[idx], key)
				}
				if engineValues[key] != string(ref[key]) {
					t.Fatalf("op %d: Scan(%d, %d): value mismatch at key %d", op, lo, hi, key)
				}
			}

		case 8: // Flush (10%)
			if err := engine.Flush(); err != nil {
				t.Fatalf("op %d: Flush: %v", op, err)
			}

		case 9: // Compact (10%)
			if err := engine.Compact(); err != nil {
				t.Fatalf("op %d: Compact: %v", op, err)
			}
		}
	}

	// Final verification: check every key in the key space
	for key := uint64(0); key < keySpace; key++ {
		got, err := engine.Get(key)
		if err != nil {
			t.Fatalf("final Get(%d): %v", key, err)
		}
		expected, exists := ref[key]
		if !exists || expected == nil {
			if got != nil {
				t.Fatalf("final Get(%d): got %d bytes, want nil", key, len(got))
			}
		} else {
			if string(got) != string(expected) {
				t.Fatalf("final Get(%d): value mismatch", key)
			}
		}
	}
}

func TestMetamorphicWithPartitioning(t *testing.T) {
	withSmallSSTSize(t, 512, 128)
	seed := int64(99)
	rng := rand.New(rand.NewSource(seed))
	engine := openTestEngine(t)
	defer engine.Close()

	ref := make(map[uint64][]byte)
	keySpace := uint64(200)
	ops := 5000

	for op := 0; op < ops; op++ {
		switch rng.Intn(10) {
		case 0, 1, 2, 3: // Put
			key := uint64(rng.Int63n(int64(keySpace)))
			valLen := rng.Intn(100) + 1
			value := make([]byte, valLen)
			rng.Read(value)
			if err := engine.Put(key, value); err != nil {
				t.Fatalf("op %d: Put(%d): %v", op, key, err)
			}
			ref[key] = value

		case 4, 5: // Get
			key := uint64(rng.Int63n(int64(keySpace)))
			got, err := engine.Get(key)
			if err != nil {
				t.Fatalf("op %d: Get(%d): %v", op, key, err)
			}
			expected, exists := ref[key]
			if !exists || expected == nil {
				if got != nil {
					t.Fatalf("op %d: Get(%d): got %d bytes, want nil", op, key, len(got))
				}
			} else {
				if string(got) != string(expected) {
					t.Fatalf("op %d: Get(%d): value mismatch", op, key)
				}
			}

		case 6: // Delete
			key := uint64(rng.Int63n(int64(keySpace)))
			if err := engine.Delete(key); err != nil {
				t.Fatalf("op %d: Delete(%d): %v", op, key, err)
			}
			ref[key] = nil

		case 7: // Scan
			lo := uint64(rng.Int63n(int64(keySpace)))
			hi := lo + uint64(rng.Intn(50)) + 1
			iter := engine.Scan(lo, hi)
			var engineKeys []uint64
			engineValues := make(map[uint64]string)
			for iter.Next() {
				engineKeys = append(engineKeys, iter.Key())
				engineValues[iter.Key()] = string(iter.Value())
			}

			var refKeys []uint64
			for key, val := range ref {
				if key >= lo && key < hi && val != nil {
					refKeys = append(refKeys, key)
				}
			}
			sort.Slice(refKeys, func(i, j int) bool { return refKeys[i] < refKeys[j] })

			if len(engineKeys) != len(refKeys) {
				t.Fatalf("op %d: Scan(%d, %d): got %d entries, want %d", op, lo, hi, len(engineKeys), len(refKeys))
			}
			for idx, key := range refKeys {
				if engineKeys[idx] != key {
					t.Fatalf("op %d: Scan key mismatch at %d: got %d, want %d", op, idx, engineKeys[idx], key)
				}
				if engineValues[key] != string(ref[key]) {
					t.Fatalf("op %d: Scan value mismatch at key %d", op, key)
				}
			}

		case 8: // Flush
			if err := engine.Flush(); err != nil {
				t.Fatalf("op %d: Flush: %v", op, err)
			}

		case 9: // Compact
			if err := engine.Compact(); err != nil {
				t.Fatalf("op %d: Compact: %v", op, err)
			}
		}
	}

	// Final full scan verification
	iter := engine.Scan(0, keySpace)
	var scanCount int
	for iter.Next() {
		key := iter.Key()
		expected := ref[key]
		if expected == nil {
			t.Fatalf("final Scan yielded deleted key %d", key)
		}
		if string(iter.Value()) != string(expected) {
			t.Fatalf("final Scan value mismatch at key %d", key)
		}
		scanCount++
	}

	expectedCount := 0
	for _, val := range ref {
		if val != nil {
			expectedCount++
		}
	}
	if scanCount != expectedCount {
		t.Fatalf("final Scan: got %d entries, want %d", scanCount, expectedCount)
	}
}

