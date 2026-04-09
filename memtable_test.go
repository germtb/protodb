package protodb

import (
	"errors"
	"testing"
)

func TestPutAndGet(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("hello"))

	got, err := m.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

func TestGetMissing(t *testing.T) {
	m := newMemtable()
	_, err := m.Get(99)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(99): expected ErrNotFound, got %v", err)
	}
}

func TestPutOverwrite(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("first"))
	m.Put(1, []byte("second"))

	got, err := m.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Get(1): got %q, want %q", got, "second")
	}
}

func TestDeleteThenGet(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("hello"))
	m.Delete(1)

	_, err := m.Get(1)
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(1) after Delete: expected ErrDeleted, got %v", err)
	}
}

func TestDeleteNonexistent(t *testing.T) {
	m := newMemtable()
	m.Delete(99)

	if m.Len() != 1 {
		t.Errorf("Len: got %d, want 1 (tombstone stored)", m.Len())
	}
	if m.ByteSize() != 0 {
		t.Errorf("ByteSize: got %d, want 0", m.ByteSize())
	}
}

func TestDeleteTwice(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("hello"))
	m.Delete(1)
	m.Delete(1)

	if m.ByteSize() != 0 {
		t.Errorf("ByteSize: got %d, want 0", m.ByteSize())
	}
}

func TestPutDeletePut(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("first"))
	m.Delete(1)
	m.Put(1, []byte("second"))

	got, err := m.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Get(1): got %q, want %q", got, "second")
	}
	if m.ByteSize() != 6 {
		t.Errorf("ByteSize: got %d, want 6", m.ByteSize())
	}
}

func TestByteSizePut(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("abc"))
	if m.ByteSize() != 3 {
		t.Errorf("ByteSize: got %d, want 3", m.ByteSize())
	}

	m.Put(2, []byte("de"))
	if m.ByteSize() != 5 {
		t.Errorf("ByteSize: got %d, want 5", m.ByteSize())
	}
}

func TestByteSizeOverwrite(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("abc"))
	m.Put(1, []byte("defgh"))

	if m.ByteSize() != 5 {
		t.Errorf("ByteSize: got %d, want 5", m.ByteSize())
	}
}

func TestByteSizeDelete(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("abc"))
	m.Delete(1)

	if m.ByteSize() != 0 {
		t.Errorf("ByteSize: got %d, want 0", m.ByteSize())
	}
}

func TestPutNilIsTombstone(t *testing.T) {
	m := newMemtable()
	m.Put(1, nil)

	_, err := m.Get(1)
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(1) after Put(nil): expected ErrDeleted, got %v", err)
	}
	if m.ByteSize() != 0 {
		t.Errorf("ByteSize: got %d, want 0", m.ByteSize())
	}
	if m.Len() != 1 {
		t.Errorf("Len: got %d, want 1", m.Len())
	}
}

func TestPutEmptyValue(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte{})

	got, err := m.Get(1)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}
}

func TestLenEmpty(t *testing.T) {
	m := newMemtable()
	if m.Len() != 0 {
		t.Errorf("Len: got %d, want 0", m.Len())
	}
}

func TestLenAfterPuts(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))
	m.Put(3, []byte("c"))

	if m.Len() != 3 {
		t.Errorf("Len: got %d, want 3", m.Len())
	}
}

func TestLenAfterDelete(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Delete(1)

	// Tombstone is still an entry
	if m.Len() != 1 {
		t.Errorf("Len: got %d, want 1", m.Len())
	}
}

func TestScanRange(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))
	m.Put(3, []byte("c"))
	m.Put(4, []byte("d"))
	m.Put(5, []byte("e"))

	var keys []uint64
	iter := m.Scan(2, 5)
	for iter.Next() {
		k := iter.Key()
		keys = append(keys, k)
	}

	if len(keys) != 3 || keys[0] != 2 || keys[1] != 3 || keys[2] != 4 {
		t.Errorf("Scan(2, 5): got %v, want [2 3 4]", keys)
	}
}

func TestMemtableScanYieldsTombstones(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))
	m.Put(3, []byte("c"))
	m.Delete(2)

	var keys []uint64
	var tombstones []uint64
	iter := m.Scan(1, 4)
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		keys = append(keys, k)
		if v == nil {
			tombstones = append(tombstones, k)
		}
	}

	if len(keys) != 3 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 {
		t.Errorf("Scan(1, 4) keys: got %v, want [1 2 3]", keys)
	}
	if len(tombstones) != 1 || tombstones[0] != 2 {
		t.Errorf("Scan(1, 4) tombstones: got %v, want [2]", tombstones)
	}
}

func TestScanEmpty(t *testing.T) {
	m := newMemtable()

	var count int = 0
	iter := m.Scan(0, 100)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan on empty: got %d entries, want 0", count)
	}
}

func TestMemtableScanNoMatch(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))

	var count int = 0
	iter := m.Scan(10, 20)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan(10, 20): got %d entries, want 0", count)
	}
}

func TestScanLoEqualsHi(t *testing.T) {
	m := newMemtable()
	m.Put(5, []byte("a"))

	var count int = 0
	iter := m.Scan(5, 5)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan(5, 5): got %d entries, want 0", count)
	}
}

func TestScanBoundaries(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))
	m.Put(3, []byte("c"))

	// lo is inclusive
	var keys []uint64
	iter := m.Scan(1, 3)
	for iter.Next() {
		k := iter.Key()
		keys = append(keys, k)
	}
	if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 {
		t.Errorf("Scan(1, 3): got %v, want [1 2]", keys)
	}
}

func TestScanBreak(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))
	m.Put(3, []byte("c"))

	var keys []uint64
	iter := m.Scan(1, 4)
	for iter.Next() {
		k := iter.Key()
		keys = append(keys, k)
		if k == 2 {
			break
		}
	}
	if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 {
		t.Errorf("Scan with break: got %v, want [1 2]", keys)
	}
}

func TestEntriesOrder(t *testing.T) {
	m := newMemtable()
	m.Put(3, []byte("c"))
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))

	var keys []uint64
	iter := m.Entries()
	for iter.Next() {
		keys = append(keys, iter.Key())
	}

	if len(keys) != 3 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 {
		t.Errorf("Entries: got %v, want [1 2 3]", keys)
	}
}

func TestEntriesIncludesTombstones(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))
	m.Delete(2)
	m.Put(3, []byte("c"))

	var keys []uint64
	var tombstones []uint64
	iter := m.Entries()
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		keys = append(keys, k)
		if v == nil {
			tombstones = append(tombstones, k)
		}
	}

	if len(keys) != 3 || keys[0] != 1 || keys[1] != 2 || keys[2] != 3 {
		t.Errorf("Entries keys: got %v, want [1 2 3]", keys)
	}
	if len(tombstones) != 1 || tombstones[0] != 2 {
		t.Errorf("Entries tombstones: got %v, want [2]", tombstones)
	}
}

func TestEntriesEmpty(t *testing.T) {
	m := newMemtable()

	var count int = 0
	iter := m.Entries()
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Entries on empty: got %d entries, want 0", count)
	}
}

func TestEntriesBreak(t *testing.T) {
	m := newMemtable()
	m.Put(1, []byte("a"))
	m.Put(2, []byte("b"))
	m.Put(3, []byte("c"))

	var keys []uint64
	iter := m.Entries()
	for iter.Next() {
		k := iter.Key()
		keys = append(keys, k)
		if k == 2 {
			break
		}
	}
	if len(keys) != 2 || keys[0] != 1 || keys[1] != 2 {
		t.Errorf("Entries with break: got %v, want [1 2]", keys)
	}
}

func TestManyEntries(t *testing.T) {
	m := newMemtable()
	for i := uint64(0); i < 1000; i++ {
		m.Put(i, []byte("value"))
	}

	if m.Len() != 1000 {
		t.Errorf("Len: got %d, want 1000", m.Len())
	}
	if m.ByteSize() != 5000 {
		t.Errorf("ByteSize: got %d, want 5000", m.ByteSize())
	}

	got, err := m.Get(500)
	if err != nil {
		t.Fatalf("Get(500): %v", err)
	}
	if string(got) != "value" {
		t.Errorf("Get(500): got %q, want %q", got, "value")
	}
}
