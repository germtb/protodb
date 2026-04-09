package protodb

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type sliceIterator struct {
	pairs []struct {
		key   uint64
		value []byte
	}
	pos int
}

func (it *sliceIterator) Next() bool {
	it.pos++
	return it.pos < len(it.pairs)
}

func (it *sliceIterator) Key() uint64 {
	return it.pairs[it.pos].key
}

func (it *sliceIterator) Value() []byte {
	return it.pairs[it.pos].value
}

func entriesFrom(pairs []struct {
	key   uint64
	value []byte
}) *sliceIterator {
	return &sliceIterator{pairs: pairs, pos: -1}
}

func writeTestSST(t *testing.T, pairs []struct {
	key   uint64
	value []byte
}) (*sst, string) {
	t.Helper()
	dir := t.TempDir()
	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}
	if len(ssts) == 0 {
		return &sst{footer: sstFooter{}, blocks: nil, hash: ""}, dir
	}
	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

func openSSTFile(t *testing.T, dir string, s *sst) *os.File {
	t.Helper()
	f, err := os.Open(filepath.Join(dir, s.hash))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.Close() })
	return f
}

// --- WriteSST / ReadSST round-trip tests ---

func TestWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("hello")},
		{2, []byte("world")},
		{3, []byte("foo")},
	}

	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}

	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		t.Fatal(err)
	}

	if s.footer.Version != sstVersion {
		t.Fatalf("version: got %d, want %d", s.footer.Version, sstVersion)
	}

	f := openSSTFile(t, dir, s)

	for _, p := range pairs {
		got, err := s.Get(p.key, f)
		if err != nil {
			t.Fatalf("Get(%d): %v", p.key, err)
		}
		if string(got) != string(p.value) {
			t.Errorf("Get(%d): got %q, want %q", p.key, got, p.value)
		}
	}
}

func TestEmptySST(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{}

	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}

	s, err := ReadSST(dir, ssts[0].hash, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.footer.BlockCount != 0 {
		t.Fatalf("block count: got %d, want 0", s.footer.BlockCount)
	}
	if len(s.blocks) != 0 {
		t.Fatalf("blocks: got %d entries, want 0", len(s.blocks))
	}
}

func TestSingleEntry(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{42, []byte("only-one")},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(42, f)
	if err != nil {
		t.Fatalf("Get(42): %v", err)
	}
	if string(got) != "only-one" {
		t.Errorf("Get(42): got %q, want %q", got, "only-one")
	}
}

func TestLargeSST(t *testing.T) {
	// 5000 entries — forces multiple blocks and exercises the block index re-read path.
	const n = 5000
	pairs := make([]struct {
		key   uint64
		value []byte
	}, n)
	for i := range pairs {
		pairs[i].key = uint64(i)
		pairs[i].value = []byte{byte(i), byte(i >> 8)}
	}

	s, dir := writeTestSST(t, pairs)
	f := openSSTFile(t, dir, s)

	for _, i := range []int{0, n / 2, n - 1} {
		got, err := s.Get(uint64(i), f)
		if err != nil {
			t.Fatalf("Get(%d): %v", i, err)
		}
		want := []byte{byte(i), byte(i >> 8)}
		if got[0] != want[0] || got[1] != want[1] {
			t.Errorf("Get(%d): got %v, want %v", i, got, want)
		}
	}
}

func TestCustomTailSize(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("aaa")},
		{2, []byte("bbb")},
		{3, []byte("ccc")},
	}

	ssts, err := WriteSST(dir, entriesFrom(pairs))
	if err != nil {
		t.Fatal(err)
	}

	// TailByteSize just enough for footer — forces re-read for block index
	s, err := ReadSST(dir, ssts[0].hash, &ReaderOptions{TailByteSize: footerSize})
	if err != nil {
		t.Fatal(err)
	}

	f := openSSTFile(t, dir, s)

	for _, p := range pairs {
		got, err := s.Get(p.key, f)
		if err != nil {
			t.Fatalf("Get(%d): %v", p.key, err)
		}
		if string(got) != string(p.value) {
			t.Errorf("Get(%d): got %q, want %q", p.key, got, p.value)
		}
	}
}

func TestReadNonExistentFile(t *testing.T) {
	_, err := ReadSST("/no/such", "nonexistent", nil)
	if err == nil {
		t.Fatal("expected error for non-existent file")
	}
}

func TestEmptyValueRoundTrip(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte{}},
		{2, []byte("between-empties")},
		{3, []byte{}},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}

	got, err = s.Get(2, f)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "between-empties" {
		t.Errorf("Get(2): got %q, want %q", got, "between-empties")
	}

	got, err = s.Get(3, f)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get(3): got len %d, want 0", len(got))
	}
}

func TestLargeValues(t *testing.T) {
	big := make([]byte, 1024*1024) // 1MB
	for i := range big {
		big[i] = byte(i % 251)
	}

	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, big},
		{2, []byte("small")},
		{3, big},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if len(got) != len(big) {
		t.Fatalf("Get(1) len: got %d, want %d", len(got), len(big))
	}
	for i := range got {
		if got[i] != big[i] {
			t.Fatalf("Get(1) byte %d: got %d, want %d", i, got[i], big[i])
		}
	}

	got, err = s.Get(2, f)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "small" {
		t.Errorf("Get(2): got %q, want %q", got, "small")
	}
}

func TestWriteUnsortedKeysError(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{3, []byte("c")},
		{1, []byte("a")},
	}
	_, err := WriteSST(dir, entriesFrom(pairs))
	if !errors.Is(err, ErrUnsortedKeys) {
		t.Fatalf("expected ErrUnsortedKeys, got %v", err)
	}
}

func TestWriteDuplicateKeysError(t *testing.T) {
	dir := t.TempDir()
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("a")},
		{1, []byte("b")},
	}
	_, err := WriteSST(dir, entriesFrom(pairs))
	if !errors.Is(err, ErrUnsortedKeys) {
		t.Fatalf("expected ErrUnsortedKeys, got %v", err)
	}
}

func TestWriteKeyZeroAllowed(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{0, []byte("zero")},
		{1, []byte("one")},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(0, f)
	if err != nil {
		t.Fatalf("Get(0): %v", err)
	}
	if string(got) != "zero" {
		t.Errorf("Get(0): got %q, want %q", got, "zero")
	}
}

func TestReadTailTooSmallError(t *testing.T) {
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
	_, err = ReadSST(dir, ssts[0].hash, &ReaderOptions{TailByteSize: 1})
	if !errors.Is(err, ErrTailTooSmall) {
		t.Fatalf("expected ErrTailTooSmall, got %v", err)
	}
}

func TestReadBadVersionError(t *testing.T) {
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

	sstPath := filepath.Join(dir, hash)
	f, err := os.OpenFile(sstPath, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	info, _ := f.Stat()
	// Version is the last 2 bytes of the file
	var badVersion uint16 = 99
	var buf [2]byte
	binary.BigEndian.PutUint16(buf[:], badVersion)
	f.WriteAt(buf[:], info.Size()-2)
	f.Close()

	_, err = ReadSST(dir, hash, nil)
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("expected ErrUnsupportedVersion, got %v", err)
	}
}

// --- Get tests ---

func TestGetFound(t *testing.T) {
	pairs := []struct {
		key   uint64
		value []byte
	}{
		{10, []byte("ten")},
		{20, []byte("twenty")},
		{30, []byte("thirty")},
	}
	s, dir := writeTestSST(t, pairs)
	f := openSSTFile(t, dir, s)

	for _, p := range pairs {
		got, err := s.Get(p.key, f)
		if err != nil {
			t.Fatalf("Get(%d): %v", p.key, err)
		}
		if string(got) != string(p.value) {
			t.Errorf("Get(%d): got %q, want %q", p.key, got, p.value)
		}
	}
}

func TestGetNotFound(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{10, []byte("ten")},
		{20, []byte("twenty")},
	})
	f := openSSTFile(t, dir, s)

	for _, missing := range []uint64{0, 5, 15, 25, 100} {
		_, err := s.Get(missing, f)
		if !errors.Is(err, ErrNotFound) {
			t.Errorf("Get(%d): expected ErrNotFound, got %v", missing, err)
		}
	}
}

func TestGetEmptySST(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{})
	f := openSSTFile(t, dir, s)

	_, err := s.Get(1, f)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get on empty SST: expected ErrNotFound, got %v", err)
	}
}

func TestGetFirstAndLastEntry(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("first")},
		{2, []byte("middle")},
		{3, []byte("last")},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "first" {
		t.Errorf("Get(1): got %q, want %q", got, "first")
	}

	got, err = s.Get(3, f)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if string(got) != "last" {
		t.Errorf("Get(3): got %q, want %q", got, "last")
	}
}

func TestGetSingleEntry(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{42, []byte("only")},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(42, f)
	if err != nil {
		t.Fatalf("Get(42): %v", err)
	}
	if string(got) != "only" {
		t.Errorf("Get(42): got %q, want %q", got, "only")
	}

	_, err = s.Get(41, f)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("Get(41): expected ErrNotFound, got %v", err)
	}
}

func TestGetEmptyValue(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte{}},
		{2, []byte("nonempty")},
		{3, []byte{}},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}

	got, err = s.Get(2, f)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "nonempty" {
		t.Errorf("Get(2): got %q, want %q", got, "nonempty")
	}

	got, err = s.Get(3, f)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get(3): got len %d, want 0", len(got))
	}
}

// --- Scan tests ---

func TestScanFullRange(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{10, []byte("a")},
		{20, []byte("b")},
		{30, []byte("c")},
	})
	f := openSSTFile(t, dir, s)

	var keys []uint64
	var vals []string
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		keys = append(keys, iter.Key())
		vals = append(vals, string(iter.Value()))
	}

	if len(keys) != 3 {
		t.Fatalf("got %d entries, want 3", len(keys))
	}
	if keys[0] != 10 || keys[1] != 20 || keys[2] != 30 {
		t.Errorf("keys: got %v, want [10 20 30]", keys)
	}
	if vals[0] != "a" || vals[1] != "b" || vals[2] != "c" {
		t.Errorf("values: got %v, want [a b c]", vals)
	}
}

func TestScanSubRange(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{10, []byte("a")},
		{20, []byte("b")},
		{30, []byte("c")},
		{40, []byte("d")},
		{50, []byte("e")},
	})
	f := openSSTFile(t, dir, s)

	var keys []uint64
	iter := s.Iterator(20, 40, f)
	for iter.Next() {
		keys = append(keys, iter.Key())
	}

	if len(keys) != 2 {
		t.Fatalf("got %d entries, want 2", len(keys))
	}
	if keys[0] != 20 || keys[1] != 30 {
		t.Errorf("keys: got %v, want [20 30]", keys)
	}
}

func TestScanEmptySST(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{})
	f := openSSTFile(t, dir, s)

	count := 0
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("got %d entries, want 0", count)
	}
}

func TestScanNoMatch(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{10, []byte("a")},
		{20, []byte("b")},
	})
	f := openSSTFile(t, dir, s)

	count := 0
	iter := s.Iterator(50, 100, f)
	for iter.Next() {
		count++
	}
	if count != 0 {
		t.Errorf("got %d entries from out-of-range scan, want 0", count)
	}
}

func TestScanSingleEntry(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{42, []byte("only")},
	})
	f := openSSTFile(t, dir, s)

	var keys []uint64
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		keys = append(keys, iter.Key())
	}
	if len(keys) != 1 || keys[0] != 42 {
		t.Errorf("got %v, want [42]", keys)
	}
}

func TestScanExactBoundaries(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{10, []byte("a")},
		{20, []byte("b")},
		{30, []byte("c")},
	})
	f := openSSTFile(t, dir, s)

	// lo is inclusive, hi is exclusive
	var keys []uint64
	iter := s.Iterator(10, 30, f)
	for iter.Next() {
		keys = append(keys, iter.Key())
	}
	if len(keys) != 2 {
		t.Fatalf("got %d entries, want 2", len(keys))
	}
	if keys[0] != 10 || keys[1] != 20 {
		t.Errorf("keys: got %v, want [10 20]", keys)
	}
}

func TestScanBreakEarly(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("a")},
		{2, []byte("b")},
		{3, []byte("c")},
		{4, []byte("d")},
		{5, []byte("e")},
	})
	f := openSSTFile(t, dir, s)

	var keys []uint64
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		keys = append(keys, iter.Key())
		if len(keys) == 2 {
			break
		}
	}
	if len(keys) != 2 {
		t.Fatalf("got %d entries, want 2", len(keys))
	}
	if keys[0] != 1 || keys[1] != 2 {
		t.Errorf("keys: got %v, want [1 2]", keys)
	}
}

func TestScanEmptyValues(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte{}},
		{2, []byte("mid")},
		{3, []byte{}},
	})
	f := openSSTFile(t, dir, s)

	var vals []string
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		vals = append(vals, string(iter.Value()))
	}
	if len(vals) != 3 {
		t.Fatalf("got %d entries, want 3", len(vals))
	}
	if vals[0] != "" || vals[1] != "mid" || vals[2] != "" {
		t.Errorf("values: got %v, want ['' 'mid' '']", vals)
	}
}

func TestScanLastEntry(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{10, []byte("first")},
		{20, []byte("last")},
	})
	f := openSSTFile(t, dir, s)

	var vals []string
	iter := s.Iterator(20, 100, f)
	for iter.Next() {
		vals = append(vals, string(iter.Value()))
	}
	if len(vals) != 1 || vals[0] != "last" {
		t.Errorf("got %v, want ['last']", vals)
	}
}

// --- Tombstone tests ---

func TestGetTombstone(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("alive")},
		{2, nil},
		{3, []byte("also alive")},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "alive" {
		t.Errorf("Get(1): got %q, want %q", got, "alive")
	}

	_, err = s.Get(2, f)
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(2): expected ErrDeleted, got %v", err)
	}

	got, err = s.Get(3, f)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if string(got) != "also alive" {
		t.Errorf("Get(3): got %q, want %q", got, "also alive")
	}
}

func TestScanYieldsTombstones(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("a")},
		{2, nil},
		{3, []byte("c")},
		{4, nil},
		{5, []byte("e")},
	})
	f := openSSTFile(t, dir, s)

	var keys []uint64
	var tombstones []uint64
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		keys = append(keys, iter.Key())
		if iter.Value() == nil {
			tombstones = append(tombstones, iter.Key())
		}
	}

	if len(keys) != 5 {
		t.Fatalf("got %d entries, want 5", len(keys))
	}
	if len(tombstones) != 2 || tombstones[0] != 2 || tombstones[1] != 4 {
		t.Errorf("tombstones: got %v, want [2 4]", tombstones)
	}
}

func TestTombstoneFirst(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, nil},
		{2, []byte("second")},
	})
	f := openSSTFile(t, dir, s)

	_, err := s.Get(1, f)
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(1): expected ErrDeleted, got %v", err)
	}

	got, err := s.Get(2, f)
	if err != nil {
		t.Fatalf("Get(2): %v", err)
	}
	if string(got) != "second" {
		t.Errorf("Get(2): got %q, want %q", got, "second")
	}
}

func TestTombstoneLast(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("first")},
		{2, nil},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if string(got) != "first" {
		t.Errorf("Get(1): got %q, want %q", got, "first")
	}

	_, err = s.Get(2, f)
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(2): expected ErrDeleted, got %v", err)
	}
}

func TestAllTombstones(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, nil},
		{2, nil},
		{3, nil},
	})
	f := openSSTFile(t, dir, s)

	for _, k := range []uint64{1, 2, 3} {
		_, err := s.Get(k, f)
		if !errors.Is(err, ErrDeleted) {
			t.Errorf("Get(%d): expected ErrDeleted, got %v", k, err)
		}
	}

	var tombstones []uint64
	iter := s.Iterator(0, 100, f)
	for iter.Next() {
		if iter.Value() != nil {
			t.Errorf("Scan(%d): expected nil value for tombstone", iter.Key())
		}
		tombstones = append(tombstones, iter.Key())
	}
	if len(tombstones) != 3 {
		t.Errorf("Scan tombstones: got %v, want [1 2 3]", tombstones)
	}
}

func TestGetMissingKeyInTombstoneSST(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte("alive")},
		{2, nil},
		{3, []byte("also alive")},
	})
	f := openSSTFile(t, dir, s)

	_, err := s.Get(99, f)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(99): expected ErrNotFound, got %v", err)
	}
}

func TestTombstoneBetweenEmptyValues(t *testing.T) {
	s, dir := writeTestSST(t, []struct {
		key   uint64
		value []byte
	}{
		{1, []byte{}},
		{2, nil},
		{3, []byte{}},
	})
	f := openSSTFile(t, dir, s)

	got, err := s.Get(1, f)
	if err != nil {
		t.Fatalf("Get(1): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get(1): got len %d, want 0", len(got))
	}

	_, err = s.Get(2, f)
	if !errors.Is(err, ErrDeleted) {
		t.Fatalf("Get(2): expected ErrDeleted, got %v", err)
	}

	got, err = s.Get(3, f)
	if err != nil {
		t.Fatalf("Get(3): %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Get(3): got len %d, want 0", len(got))
	}
}
