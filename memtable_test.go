package protodb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

func key(k uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, k)
	return buf
}

func TestPutAndGet(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("hello"))

	got, err := m.Get(key(1))
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
}

func TestGetMissing(t *testing.T) {
	m := newMemtable()
	_, err := m.Get(key(99))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(99): expected ErrNotFound, got %v", err)
	}
}

func TestPutOverwrite(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("first"))
	m.Put(key(1), []byte("second"))

	got, err := m.Get(key(1))
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Get(1): got %q, want %q", got, "second")
	}
}

func TestDeleteThenGet(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("hello"))
	m.Delete(key(1))

	_, err := m.Get(key(1))
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(1) after Delete: expected ErrDeleted, got %v", err)
	}
}

func TestDeleteNonexistent(t *testing.T) {
	m := newMemtable()
	m.Delete(key(99))

	if m.Len() != 1 {
		t.Errorf("Len: got %d, want 1 (tombstone stored)", m.Len())
	}
	if m.ByteSize() != 8 {
		t.Errorf("ByteSize: got %d, want 8 (key only)", m.ByteSize())
	}
}

func TestDeleteTwice(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("hello"))
	m.Delete(key(1))
	m.Delete(key(1))

	if m.ByteSize() != 8 {
		t.Errorf("ByteSize: got %d, want 8 (key only)", m.ByteSize())
	}
}

func TestPutDeletePut(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("first"))
	m.Delete(key(1))
	m.Put(key(1), []byte("second"))

	got, err := m.Get(key(1))
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Get(1): got %q, want %q", got, "second")
	}
	if m.ByteSize() != 14 {
		t.Errorf("ByteSize: got %d, want 14 (8 key + 6 value)", m.ByteSize())
	}
}

func TestByteSizePut(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("abc"))
	if m.ByteSize() != 11 {
		t.Errorf("ByteSize: got %d, want 11 (8 key + 3 value)", m.ByteSize())
	}

	m.Put(key(2), []byte("de"))
	if m.ByteSize() != 21 {
		t.Errorf("ByteSize: got %d, want 21 (2*8 key + 3+2 value)", m.ByteSize())
	}
}

func TestByteSizeOverwrite(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("abc"))
	m.Put(key(1), []byte("defgh"))

	if m.ByteSize() != 13 {
		t.Errorf("ByteSize: got %d, want 13 (8 key + 5 value)", m.ByteSize())
	}
}

func TestByteSizeDelete(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("abc"))
	m.Delete(key(1))

	if m.ByteSize() != 8 {
		t.Errorf("ByteSize: got %d, want 8 (key only, tombstone)", m.ByteSize())
	}
}

func TestPutNilIsTombstone(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), nil)

	_, err := m.Get(key(1))
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(1) after Put(nil): expected ErrDeleted, got %v", err)
	}
	if m.ByteSize() != 8 {
		t.Errorf("ByteSize: got %d, want 8 (key only)", m.ByteSize())
	}
	if m.Len() != 1 {
		t.Errorf("Len: got %d, want 1", m.Len())
	}
}

func TestPutEmptyValue(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte{})

	got, err := m.Get(key(1))
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
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))
	m.Put(key(3), []byte("c"))

	if m.Len() != 3 {
		t.Errorf("Len: got %d, want 3", m.Len())
	}
}

func TestLenAfterDelete(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("a"))
	m.Delete(key(1))

	// Tombstone is still an entry
	if m.Len() != 1 {
		t.Errorf("Len: got %d, want 1", m.Len())
	}
}

func TestScanRange(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))
	m.Put(key(3), []byte("c"))
	m.Put(key(4), []byte("d"))
	m.Put(key(5), []byte("e"))

	var keys [][]byte
	iter := m.Scan(key(2), key(5))
	for iter.Next() {
		keys = append(keys, iter.Key())
	}

	if len(keys) != 3 || !bytes.Equal(keys[0], key(2)) || !bytes.Equal(keys[1], key(3)) || !bytes.Equal(keys[2], key(4)) {
		t.Errorf("Scan(2, 5): got %v, want [2 3 4]", keys)
	}
}

func TestMemtableScanYieldsTombstones(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))
	m.Put(key(3), []byte("c"))
	m.Delete(key(2))

	var keys [][]byte
	var tombstones [][]byte
	iter := m.Scan(key(1), key(4))
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		keys = append(keys, k)
		if v == nil {
			tombstones = append(tombstones, k)
		}
	}

	if len(keys) != 3 || !bytes.Equal(keys[0], key(1)) || !bytes.Equal(keys[1], key(2)) || !bytes.Equal(keys[2], key(3)) {
		t.Errorf("Scan(1, 4) keys: got %v, want [1 2 3]", keys)
	}
	if len(tombstones) != 1 || !bytes.Equal(tombstones[0], key(2)) {
		t.Errorf("Scan(1, 4) tombstones: got %v, want [2]", tombstones)
	}
}

func TestScanEmpty(t *testing.T) {
	m := newMemtable()

	var count int = 0
	iter := m.Scan(key(0), key(100))
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan on empty: got %d entries, want 0", count)
	}
}

func TestMemtableScanNoMatch(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))

	var count int = 0
	iter := m.Scan(key(10), key(20))
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan(10, 20): got %d entries, want 0", count)
	}
}

func TestScanLoEqualsHi(t *testing.T) {
	m := newMemtable()
	m.Put(key(5), []byte("a"))

	var count int = 0
	iter := m.Scan(key(5), key(5))
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("Scan(5, 5): got %d entries, want 0", count)
	}
}

func TestScanBoundaries(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))
	m.Put(key(3), []byte("c"))

	// lo is inclusive
	var keys [][]byte
	iter := m.Scan(key(1), key(3))
	for iter.Next() {
		keys = append(keys, iter.Key())
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], key(1)) || !bytes.Equal(keys[1], key(2)) {
		t.Errorf("Scan(1, 3): got %v, want [1 2]", keys)
	}
}

func TestScanBreak(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))
	m.Put(key(3), []byte("c"))

	var keys [][]byte
	iter := m.Scan(key(1), key(4))
	for iter.Next() {
		k := iter.Key()
		keys = append(keys, k)
		if bytes.Equal(k, key(2)) {
			break
		}
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], key(1)) || !bytes.Equal(keys[1], key(2)) {
		t.Errorf("Scan with break: got %v, want [1 2]", keys)
	}
}

func TestEntriesOrder(t *testing.T) {
	m := newMemtable()
	m.Put(key(3), []byte("c"))
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))

	var keys [][]byte
	iter := m.Entries()
	for iter.Next() {
		keys = append(keys, iter.Key())
	}

	if len(keys) != 3 || !bytes.Equal(keys[0], key(1)) || !bytes.Equal(keys[1], key(2)) || !bytes.Equal(keys[2], key(3)) {
		t.Errorf("Entries: got %v, want [1 2 3]", keys)
	}
}

func TestEntriesIncludesTombstones(t *testing.T) {
	m := newMemtable()
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))
	m.Delete(key(2))
	m.Put(key(3), []byte("c"))

	var keys [][]byte
	var tombstones [][]byte
	iter := m.Entries()
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		keys = append(keys, k)
		if v == nil {
			tombstones = append(tombstones, k)
		}
	}

	if len(keys) != 3 || !bytes.Equal(keys[0], key(1)) || !bytes.Equal(keys[1], key(2)) || !bytes.Equal(keys[2], key(3)) {
		t.Errorf("Entries keys: got %v, want [1 2 3]", keys)
	}
	if len(tombstones) != 1 || !bytes.Equal(tombstones[0], key(2)) {
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
	m.Put(key(1), []byte("a"))
	m.Put(key(2), []byte("b"))
	m.Put(key(3), []byte("c"))

	var keys [][]byte
	iter := m.Entries()
	for iter.Next() {
		k := iter.Key()
		keys = append(keys, k)
		if bytes.Equal(k, key(2)) {
			break
		}
	}
	if len(keys) != 2 || !bytes.Equal(keys[0], key(1)) || !bytes.Equal(keys[1], key(2)) {
		t.Errorf("Entries with break: got %v, want [1 2]", keys)
	}
}

func TestManyEntries(t *testing.T) {
	m := newMemtable()
	for idx := uint64(0); idx < 1000; idx++ {
		m.Put(key(idx), []byte("value"))
	}

	if m.Len() != 1000 {
		t.Errorf("Len: got %d, want 1000", m.Len())
	}
	if m.ByteSize() != 13000 {
		t.Errorf("ByteSize: got %d, want 13000 (1000 * (8 key + 5 value))", m.ByteSize())
	}

	got, err := m.Get(key(500))
	if err != nil {
		t.Fatalf("Get(500): %v", err)
	}
	if string(got) != "value" {
		t.Errorf("Get(500): got %q, want %q", got, "value")
	}
}
