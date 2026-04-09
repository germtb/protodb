package protodb

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

// =============================================================================
// --- WAL unit tests ---
// =============================================================================

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, err := newWAL(walPath)
	if err != nil {
		t.Fatal(err)
	}

	wal.Append(1, []byte("hello"))
	wal.Append(2, []byte("world"))
	wal.Close()

	table := newMemtable()
	err = replayWAL(walPath, &table)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := table.Get(1)
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
	got, _ = table.Get(2)
	if string(got) != "world" {
		t.Errorf("Get(2): got %q, want %q", got, "world")
	}
}

func TestWALReplayTombstone(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("alive"))
	wal.Append(1, nil) // delete
	wal.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	_, err := table.Get(1)
	if err != ErrDeleted {
		t.Fatalf("expected ErrDeleted, got %v", err)
	}
}

func TestWALReplayEmptyValue(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte{})
	wal.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	got, err := table.Get(1)
	if err != nil {
		t.Fatal(err)
	}
	if got == nil {
		t.Fatal("got nil, want empty slice")
	}
	if len(got) != 0 {
		t.Errorf("got len %d, want 0", len(got))
	}
}

func TestWALReplayOverwrite(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("first"))
	wal.Append(1, []byte("second"))
	wal.Append(1, []byte("third"))
	wal.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	got, _ := table.Get(1)
	if string(got) != "third" {
		t.Errorf("got %q, want %q", got, "third")
	}
}

func TestWALReplayNonexistentFile(t *testing.T) {
	table := newMemtable()
	err := replayWAL("/no/such/file", &table)
	if err != nil {
		t.Fatalf("expected nil error for nonexistent WAL, got %v", err)
	}
	if table.Len() != 0 {
		t.Errorf("expected empty memtable, got %d entries", table.Len())
	}
}

func TestWALReplayEmptyFile(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	os.WriteFile(walPath, []byte{}, 0644)

	table := newMemtable()
	err := replayWAL(walPath, &table)
	if err != nil {
		t.Fatal(err)
	}
	if table.Len() != 0 {
		t.Errorf("expected empty memtable, got %d entries", table.Len())
	}
}

func TestWALReplayTruncatedFrameLen(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	// Only 2 bytes — not enough for a frame_len (u32)
	os.WriteFile(walPath, []byte{0x00, 0x01}, 0644)

	table := newMemtable()
	err := replayWAL(walPath, &table)
	if err != nil {
		t.Fatal(err)
	}
	if table.Len() != 0 {
		t.Errorf("expected empty memtable, got %d entries", table.Len())
	}
}

func TestWALReplayTruncatedChecksum(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")
	// Valid frame_len but checksum is cut short
	var buf [6]byte
	binary.BigEndian.PutUint32(buf[0:4], 100)
	os.WriteFile(walPath, buf[:], 0644)

	table := newMemtable()
	err := replayWAL(walPath, &table)
	if err != nil {
		t.Fatal(err)
	}
	if table.Len() != 0 {
		t.Errorf("expected empty memtable, got %d entries", table.Len())
	}
}

func TestWALReplayTruncatedPayload(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	// Write one good entry, then a header claiming a large payload that's truncated
	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("good"))
	wal.Close()

	// Append a header that claims 100 bytes of payload but write nothing
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	var header [walHeaderSize]byte
	binary.BigEndian.PutUint32(header[0:4], 100)
	binary.BigEndian.PutUint32(header[4:8], 0xDEADBEEF)
	f.Write(header[:])
	f.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	// First entry should survive
	got, _ := table.Get(1)
	if string(got) != "good" {
		t.Errorf("got %q, want %q", got, "good")
	}
	if table.Len() != 1 {
		t.Errorf("expected 1 entry, got %d", table.Len())
	}
}

func TestWALReplayBadChecksum(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("good"))
	wal.Append(2, []byte("bad"))
	wal.Append(3, []byte("after bad"))
	wal.Close()

	data, _ := os.ReadFile(walPath)
	// Corrupt the checksum of the second frame (byte 5 of second frame)
	// First frame size: 4 (frame_len) + 4 (crc) + 8 (key) + 8 (len) + 4 (value "good") = 28
	data[28+5] ^= 0xFF
	os.WriteFile(walPath, data, 0644)

	table := newMemtable()
	replayWAL(walPath, &table)

	got, _ := table.Get(1)
	if string(got) != "good" {
		t.Errorf("got %q, want %q", got, "good")
	}
	// Entries 2 and 3 should be lost
	_, err := table.Get(2)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for key 2, got %v", err)
	}
	_, err = table.Get(3)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for key 3, got %v", err)
	}
}

func TestWALReplayFrameLenZero(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("good"))
	wal.Close()

	// Append a frame with frame_len = 0
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	var header [walHeaderSize]byte // all zeros
	f.Write(header[:])
	f.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	got, _ := table.Get(1)
	if string(got) != "good" {
		t.Errorf("got %q, want %q", got, "good")
	}
}

func TestWALReplayGarbage(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	garbage := make([]byte, 1024)
	rand.Read(garbage)
	os.WriteFile(walPath, garbage, 0644)

	table := newMemtable()
	err := replayWAL(walPath, &table)
	if err != nil {
		t.Fatal(err)
	}
	// Random garbage is overwhelmingly unlikely to produce a valid CRC frame.
	// We just verify it doesn't panic.
}

func TestWALReplayTruncatesToLastGood(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("good"))
	wal.Close()

	beforeSize := fileSize(t, walPath)

	// Append garbage after the valid frame
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte("this is garbage that should be truncated"))
	f.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	afterSize := fileSize(t, walPath)
	if afterSize != beforeSize {
		t.Errorf("WAL should be truncated to %d bytes, got %d", beforeSize, afterSize)
	}
}

func TestWALClear(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("hello"))
	wal.Append(2, []byte("world"))
	wal.Clear()

	// WAL should be empty
	info, _ := os.Stat(walPath)
	if info.Size() != 0 {
		t.Errorf("WAL size after Clear: got %d, want 0", info.Size())
	}

	// Replay should yield nothing
	table := newMemtable()
	replayWAL(walPath, &table)
	if table.Len() != 0 {
		t.Errorf("expected empty memtable after replay of cleared WAL, got %d", table.Len())
	}
}

func TestWALClearThenAppend(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("before"))
	wal.Clear()
	wal.Append(2, []byte("after"))
	wal.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	_, err := table.Get(1)
	if err != ErrNotFound {
		t.Errorf("key 1 should not exist after Clear, got %v", err)
	}
	got, _ := table.Get(2)
	if string(got) != "after" {
		t.Errorf("got %q, want %q", got, "after")
	}
}

func TestWALLargeValue(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	big := make([]byte, 1024*1024) // 1MB
	for idx := range big {
		big[idx] = byte(idx % 251)
	}

	wal, _ := newWAL(walPath)
	wal.Append(1, big)
	wal.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	got, _ := table.Get(1)
	if len(got) != len(big) {
		t.Fatalf("got len %d, want %d", len(got), len(big))
	}
	for idx := range got {
		if got[idx] != big[idx] {
			t.Fatalf("byte %d: got %d, want %d", idx, got[idx], big[idx])
		}
	}
}

func TestWALManyEntries(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	for idx := uint64(0); idx < 10000; idx++ {
		wal.Append(idx, []byte("v"))
	}
	wal.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	if table.Len() != 10000 {
		t.Errorf("got %d entries, want 10000", table.Len())
	}
}

func TestWALOnlyTombstones(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, nil)
	wal.Append(2, nil)
	wal.Append(3, nil)
	wal.Close()

	table := newMemtable()
	replayWAL(walPath, &table)

	for _, key := range []uint64{1, 2, 3} {
		_, err := table.Get(key)
		if err != ErrDeleted {
			t.Errorf("Get(%d): expected ErrDeleted, got %v", key, err)
		}
	}
}

func TestWALNewOnUnwritableDir(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0555)
	defer os.Chmod(dir, 0755)

	_, err := newWAL(filepath.Join(dir, "wal"))
	if err == nil {
		t.Fatal("expected error creating WAL in unwritable dir")
	}
}

func TestWALReplayUnreadableFile(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(walPath)
	wal.Append(1, []byte("hello"))
	wal.Close()

	os.Chmod(walPath, 0000)
	defer os.Chmod(walPath, 0644)

	table := newMemtable()
	err := replayWAL(walPath, &table)
	if err == nil {
		t.Fatal("expected error replaying unreadable WAL")
	}
}

func TestWALDoubleClose(t *testing.T) {
	dir := t.TempDir()
	wal, _ := newWAL(filepath.Join(dir, "wal"))
	wal.Close()
	// Second close should return an error, not panic
	err := wal.Close()
	if err == nil {
		t.Fatal("expected error on double close")
	}
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	return info.Size()
}
