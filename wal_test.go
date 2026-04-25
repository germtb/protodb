package protodb

import (
	"encoding/binary"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
)

func walKey(k uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, k)
	return buf
}

// =============================================================================
// --- WAL unit tests ---
// =============================================================================

func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, err := newWAL(DefaultFS, walPath)
	if err != nil {
		t.Fatal(err)
	}

	wal.Append(walKey(1), []byte("hello"))
	wal.Append(walKey(2), []byte("world"))
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	_, err = wal2.replay(table)
	if err != nil {
		t.Fatal(err)
	}

	got, _ := table.Get(walKey(1), VisibleAll)
	if string(got) != "hello" {
		t.Errorf("Get(1): got %q, want %q", got, "hello")
	}
	got, _ = table.Get(walKey(2), VisibleAll)
	if string(got) != "world" {
		t.Errorf("Get(2): got %q, want %q", got, "world")
	}
}

func TestWALReplayTombstone(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("alive"))
	wal.Append(walKey(1), nil) // delete
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	_, err := table.Get(walKey(1), VisibleAll)
	if err != ErrDeleted {
		t.Fatalf("expected ErrDeleted, got %v", err)
	}
}

func TestWALReplayEmptyValue(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte{})
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	got, err := table.Get(walKey(1), VisibleAll)
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

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("first"))
	wal.Append(walKey(1), []byte("second"))
	wal.Append(walKey(1), []byte("third"))
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	got, _ := table.Get(walKey(1), VisibleAll)
	if string(got) != "third" {
		t.Errorf("got %q, want %q", got, "third")
	}
}

func TestWALReplayNonexistentFile(t *testing.T) {
	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, "/no/such/file")
	_, err := wal2.replay(table)
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
	wal2, _ := newWAL(DefaultFS, walPath)
	_, err := wal2.replay(table)
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
	wal2, _ := newWAL(DefaultFS, walPath)
	_, err := wal2.replay(table)
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
	wal2, _ := newWAL(DefaultFS, walPath)
	_, err := wal2.replay(table)
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
	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("good"))
	wal.Close()

	// Append a checksum followed by truncated payload (no key_len)
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	var crc [walChecksumSize]byte
	binary.BigEndian.PutUint32(crc[:], 0xDEADBEEF)
	f.Write(crc[:])
	f.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	// First entry should survive
	got, _ := table.Get(walKey(1), VisibleAll)
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

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("good"))
	wal.Append(walKey(2), []byte("bad"))
	wal.Append(walKey(3), []byte("after bad"))
	wal.Close()

	data, _ := os.ReadFile(walPath)
	// Layout: [frame1 24B][commit 8B][frame2 starts at 32]
	// Corrupt a byte inside frame2's CRC (offset 32..35).
	data[33] ^= 0xFF
	os.WriteFile(walPath, data, 0644)

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	got, _ := table.Get(walKey(1), VisibleAll)
	if string(got) != "good" {
		t.Errorf("got %q, want %q", got, "good")
	}
	// Entries 2 and 3 should be lost
	_, err := table.Get(walKey(2), VisibleAll)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for key 2, got %v", err)
	}
	_, err = table.Get(walKey(3), VisibleAll)
	if err != ErrNotFound {
		t.Errorf("expected ErrNotFound for key 3, got %v", err)
	}
}

func TestWALReplayFrameLenZero(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("good"))
	wal.Close()

	// Append a zero checksum with no payload
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	var header [walChecksumSize]byte // all zeros
	f.Write(header[:])
	f.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	got, _ := table.Get(walKey(1), VisibleAll)
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
	wal2, _ := newWAL(DefaultFS, walPath)
	_, err := wal2.replay(table)
	if err != nil {
		t.Fatal(err)
	}
	// Random garbage is overwhelmingly unlikely to produce a valid CRC frame.
	// We just verify it doesn't panic.
}

func TestWALReplayIgnoresTrailingGarbage(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("good"))
	wal.Close()

	// Append garbage after the valid frame
	f, _ := os.OpenFile(walPath, os.O_APPEND|os.O_WRONLY, 0644)
	f.Write([]byte("this is garbage that should be ignored"))
	f.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	got, _ := table.Get(walKey(1), VisibleAll)
	if string(got) != "good" {
		t.Errorf("expected 'good', got %q", got)
	}
}

func TestWALClear(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("hello"))
	wal.Append(walKey(2), []byte("world"))
	wal.Clear()

	// WAL should be empty. With the buffered WAL, small writes may never have
	// reached disk, in which case the file simply doesn't exist — also "empty".
	info, err := os.Stat(walPath)
	if err == nil && info.Size() != 0 {
		t.Errorf("WAL size after Clear: got %d, want 0", info.Size())
	} else if err != nil && !os.IsNotExist(err) {
		t.Fatalf("stat WAL: %v", err)
	}

	// Replay should yield nothing
	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore
	if table.Len() != 0 {
		t.Errorf("expected empty memtable after replay of cleared WAL, got %d", table.Len())
	}
}

func TestWALClearThenAppend(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("before"))
	wal.Clear()
	wal.Append(walKey(2), []byte("after"))
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	_, err := table.Get(walKey(1), VisibleAll)
	if err != ErrNotFound {
		t.Errorf("key 1 should not exist after Clear, got %v", err)
	}
	got, _ := table.Get(walKey(2), VisibleAll)
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

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), big)
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	got, _ := table.Get(walKey(1), VisibleAll)
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

	wal, _ := newWAL(DefaultFS, walPath)
	for idx := uint64(0); idx < 10000; idx++ {
		wal.Append(walKey(idx), []byte("v"))
	}
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	if table.Len() != 10000 {
		t.Errorf("got %d entries, want 10000", table.Len())
	}
}

func TestWALOnlyTombstones(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), nil)
	wal.Append(walKey(2), nil)
	wal.Append(walKey(3), nil)
	wal.Close()

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	wal2.replay(table) // ignore

	for _, k := range []uint64{1, 2, 3} {
		_, err := table.Get(walKey(k), VisibleAll)
		if err != ErrDeleted {
			t.Errorf("Get(%d): expected ErrDeleted, got %v", k, err)
		}
	}
}

func TestWALWriteOnUnwritableDir(t *testing.T) {
	dir := t.TempDir()
	os.Chmod(dir, 0555)
	defer os.Chmod(dir, 0755)

	wal, err := newWAL(DefaultFS, filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatal(err)
	}
	// Buffered WAL: Append doesn't touch disk until the flush threshold is
	// crossed. Write enough bytes to force a flush attempt and observe the
	// resulting error. Size the workload relative to the current threshold
	// so bumping DefaultWALFlushBytes doesn't silently under-test.
	bigVal := make([]byte, 1024)
	iterations := DefaultWALFlushBytes/len(bigVal) + 64
	for idx := 0; idx < iterations; idx++ {
		err = wal.Append(walKey(uint64(idx)), bigVal)
		if err != nil {
			return // got expected error
		}
	}
	t.Fatal("expected error writing WAL in unwritable dir")
}

func TestWALReplayUnreadableFile(t *testing.T) {
	dir := t.TempDir()
	walPath := filepath.Join(dir, "wal")

	wal, _ := newWAL(DefaultFS, walPath)
	wal.Append(walKey(1), []byte("hello"))
	wal.Close()

	os.Chmod(walPath, 0000)
	defer os.Chmod(walPath, 0644)

	table := newMemtable()
	wal2, _ := newWAL(DefaultFS, walPath)
	_, err := wal2.replay(table)
	if err == nil {
		t.Fatal("expected error replaying unreadable WAL")
	}
}

func TestWALDoubleClose(t *testing.T) {
	dir := t.TempDir()
	wal, _ := newWAL(DefaultFS, filepath.Join(dir, "wal"))
	wal.Append(walKey(1), []byte("hello"))
	wal.Close()
	// Second close should not panic
	err := wal.Close()
	if err != nil {
		t.Fatalf("expected nil on double close, got %v", err)
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

// TestWALPartialTransaction verifies WAL atomicity: a multi-entry
// transaction whose tail is torn by a crash must NOT replay as a partial
// prefix. After the fix, replay either applies the whole batch or none of
// it.
//
// Repro: write 5 keys in one Transaction.Apply(), close, truncate the
// last 5 bytes of the WAL file, reopen, assert all 5 present OR 0 present
// (never 4).
func TestWALPartialTransaction(t *testing.T) {
	dir := t.TempDir()

	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	tx := engine.Transaction()
	for i := uint64(1); i <= 5; i++ {
		tx.Put(walKey(i), []byte("value"))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	// Truncate the last 5 bytes of the WAL — simulates a torn write at the
	// tail of a multi-frame batch.
	walPath := filepath.Join(dir, "protodb", "wal")
	info, err := os.Stat(walPath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(walPath, info.Size()-5); err != nil {
		t.Fatal(err)
	}

	// Reopen — WAL replay must give all-or-nothing.
	engine, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	present := 0
	for i := uint64(1); i <= 5; i++ {
		v, err := engine.Get(walKey(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if v != nil {
			present++
		}
	}

	if present != 0 && present != 5 {
		t.Fatalf("WAL atomicity violated: %d of 5 keys present after torn write (expected 0 or 5)", present)
	}
}

// TestWALBatchMidCorruption corrupts a Middle frame of a 5-entry batch
// and asserts no entries from that batch survive replay.
func TestWALBatchMidCorruption(t *testing.T) {
	dir := t.TempDir()

	engine, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}

	tx := engine.Transaction()
	for i := uint64(1); i <= 5; i++ {
		tx.Put(walKey(i), []byte("value"))
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := engine.Close(); err != nil {
		t.Fatal(err)
	}

	walPath := filepath.Join(dir, "protodb", "wal")
	data, err := os.ReadFile(walPath)
	if err != nil {
		t.Fatal(err)
	}
	// Each frame: 4 (crc) + 4 (key_len) + 8 (key) + 4 (value_len) + 5 (value) = 25 bytes.
	// Plus a 4-byte commit marker at the end of the batch.
	const frameSize = 25
	if len(data) < 5*frameSize+4 {
		t.Fatalf("unexpected WAL size %d", len(data))
	}
	// Flip a byte mid-payload of frame index 2 (third frame) so the CRC
	// fails on it.
	corruptOffset := 2*frameSize + 10
	data[corruptOffset] ^= 0xFF
	if err := os.WriteFile(walPath, data, 0644); err != nil {
		t.Fatal(err)
	}

	engine, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	present := 0
	for i := uint64(1); i <= 5; i++ {
		v, err := engine.Get(walKey(i))
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		if v != nil {
			present++
		}
	}

	if present != 0 {
		t.Fatalf("expected 0 keys after mid-batch corruption (all-or-nothing), got %d", present)
	}
}
