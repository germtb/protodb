package protodb

import (
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"sync"
)

type Policy struct {
	SoftCompactionThreshold int
	HardCompactionThreshold int
	FlushThreshold          int
}

type Engine struct {
	flushMutex      sync.RWMutex
	compactionMutex sync.RWMutex
	memtable        *memtable
	path            string
	fileTable       *FileTable
	wal             *WAL
	manifest        *Manifest
	l0ssts          []*sst
	l1ssts          []*sst
	policy          *Policy

	// Counters (updated under flushMutex / compactionMutex respectively).
	flushCount      uint64
	compactionCount uint64
}

func loadSSTs(objectsPath string, hashes []string) ([]*sst, error) {
	ssts := make([]*sst, 0, len(hashes))
	for _, h := range hashes {
		s, err := ReadSST(objectsPath, h, nil)
		if err != nil {
			return nil, err
		}
		ssts = append(ssts, s)
	}
	return ssts, nil
}

func Open(path string) (*Engine, error) {
	path = filepath.Join(path, "protodb")

	err := os.MkdirAll(filepath.Join(path, "objects"), 0755)
	if err != nil {
		return nil, err
	}

	wal, err := newWAL(filepath.Join(path, "wal"))
	if err != nil {
		return nil, err
	}

	manifest, err := newManifest(filepath.Join(path, "manifest"))
	if err != nil {
		return nil, err
	}

	objectsPath := filepath.Join(path, "objects")
	l0ssts, err := loadSSTs(objectsPath, manifest.L0Hashes())
	if err != nil {
		return nil, err
	}
	l1ssts, err := loadSSTs(objectsPath, manifest.L1Hashes())
	if err != nil {
		return nil, err
	}

	memtable := newMemtable()
	_, err = wal.replay(memtable)

	if err != nil {
		return nil, err
	}

	e := &Engine{
		memtable:  memtable,
		path:      path,
		fileTable: newFileTable(128),
		wal:       wal,
		manifest:  manifest,
		l0ssts:    l0ssts,
		l1ssts:    l1ssts,
		policy: &Policy{
			SoftCompactionThreshold: 4,                // 4 L0 ssts
			HardCompactionThreshold: 16,               // 16 L0 ssts
			FlushThreshold:          1024 * 1024 * 64, // 64Mb
		},
	}

	// Sweep any SST files orphaned by a crash between compaction's manifest
	// sync and its inline os.Remove calls. Cheap — runs once per Open.
	if err := e.gcLocked(); err != nil {
		return nil, err
	}
	return e, nil
}

func (e *Engine) WALPath() string {
	return filepath.Join(e.path, "wal")
}

func (e *Engine) ObjectsPath() string {
	return filepath.Join(e.path, "objects")
}

func (e *Engine) SetPolicy(policy *Policy) {
	e.policy = policy
}

// EngineStats is a snapshot of operational counters + level sizes.
type EngineStats struct {
	FlushCount      uint64
	CompactionCount uint64
	L0SSTs          int
	L0Bytes         int64
	L1SSTs          int
	L1Bytes         int64
}

// Stats returns a snapshot of counters and level sizes.
func (e *Engine) Stats() EngineStats {
	e.flushMutex.RLock()
	defer e.flushMutex.RUnlock()
	var l0Bytes, l1Bytes int64
	for _, s := range e.l0ssts {
		l0Bytes += s.fileSize
	}
	for _, s := range e.l1ssts {
		l1Bytes += s.fileSize
	}
	return EngineStats{
		FlushCount:      e.flushCount,
		CompactionCount: e.compactionCount,
		L0SSTs:          len(e.l0ssts),
		L0Bytes:         l0Bytes,
		L1SSTs:          len(e.l1ssts),
		L1Bytes:         l1Bytes,
	}
}

func (e *Engine) Close() error {
	e.compactionMutex.Lock()
	defer e.compactionMutex.Unlock()
	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()
	e.fileTable.Clear()
	return e.wal.Close()
}

func (e *Engine) Put(key Key, value []byte) error {
	tx := e.Transaction()
	tx.Put(key, value)
	return tx.Commit()
}

func (e *Engine) Delete(key Key) error {
	tx := e.Transaction()
	tx.Delete(key)
	return tx.Commit()
}

func (e *Engine) GetInSST(s *sst, key Key) ([]byte, error) {
	handle, err := e.fileTable.getOrOpen(s.path)
	if err != nil {
		return nil, err
	}
	defer handle.Close()

	value, err := s.Get(key, handle)

	if errors.Is(err, ErrNotFound) {
		return nil, ErrNotFound
	} else if errors.Is(err, ErrDeleted) {
		return nil, ErrDeleted
	} else if err != nil {
		return nil, err
	} else {
		return value, err
	}
}

func (e *Engine) Get(key Key) ([]byte, error) {
	e.flushMutex.RLock()
	defer e.flushMutex.RUnlock()
	return e.getLocked(key)
}

// BulkGet retrieves multiple keys in a single operation.
// Returns values aligned with the input keys: result[i] is the value for keys[i].
// Missing keys (and tombstones) have nil values in the result.
//
// The implementation processes keys in sorted order so each storage source
// (memtable, L0 SSTs, L1 SSTs) is walked at most once across the entire batch.
func (e *Engine) BulkGet(keys []Key) ([][]byte, error) {
	n := len(keys)
	if n == 0 {
		return nil, nil
	}

	e.flushMutex.RLock()
	defer e.flushMutex.RUnlock()

	// Build a permutation that sorts the keys without copying them.
	// indices[i] is the original index of the i-th sorted key.
	indices := make([]int, n)
	for i := range indices {
		indices[i] = i
	}
	sort.Slice(indices, func(i, j int) bool {
		return bytes.Compare(keys[indices[i]], keys[indices[j]]) < 0
	})

	// Build sortedKeys slice for SST.BulkGet which requires a sorted []Key.
	sortedKeys := make([]Key, n)
	for i := range n {
		sortedKeys[i] = keys[indices[i]]
	}

	// Parallel arrays aligned with sortedKeys.
	sortedDst := make([][]byte, n)
	resolved := make([]bool, n)

	// 1. Memtable (highest precedence)
	for i := range n {
		value, err := e.memtable.Get(sortedKeys[i])
		if err == nil {
			sortedDst[i] = value
			resolved[i] = true
		} else if errors.Is(err, ErrDeleted) {
			resolved[i] = true // tombstone — don't fall through
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	// 2. L0 SSTs (newest first)
	for _, s := range e.l0ssts {
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			return nil, err
		}
		values, errs, err := s.BulkGet(sortedKeys, handle)
		handle.Close()
		if err != nil {
			return nil, err
		}
		for i := range n {
			if resolved[i] {
				continue
			}
			if errs[i] == nil {
				sortedDst[i] = values[i]
				resolved[i] = true
			} else if errors.Is(errs[i], ErrDeleted) {
				resolved[i] = true
			}
		}
	}

	// 3. L1 SSTs (sorted, non-overlapping). Bucket remaining keys by SST.
	startIdx := 0
	for keyIdx := 0; keyIdx < n; {
		if resolved[keyIdx] {
			keyIdx++
			continue
		}

		k := sortedKeys[keyIdx]

		// Binary search [startIdx:] for the SST containing k
		ssts := e.l1ssts[startIdx:]
		offset := sort.Search(len(ssts), func(i int) bool {
			return bytes.Compare(ssts[i].firstKey, k) > 0
		}) - 1
		sstIdx := startIdx + offset

		if sstIdx < 0 {
			keyIdx++
			continue
		}

		// Determine how many subsequent keys belong to this SST.
		var nextFirstKey Key
		if sstIdx+1 < len(e.l1ssts) {
			nextFirstKey = e.l1ssts[sstIdx+1].firstKey
		}

		batchEnd := keyIdx + 1
		for batchEnd < n {
			if nextFirstKey != nil && bytes.Compare(sortedKeys[batchEnd], nextFirstKey) >= 0 {
				break
			}
			batchEnd++
		}

		s := e.l1ssts[sstIdx]
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			return nil, err
		}
		values, errs, err := s.BulkGet(sortedKeys[keyIdx:batchEnd], handle)
		handle.Close()
		if err != nil {
			return nil, err
		}
		for i := keyIdx; i < batchEnd; i++ {
			if resolved[i] {
				continue
			}
			localIdx := i - keyIdx
			if errs[localIdx] == nil {
				sortedDst[i] = values[localIdx]
				resolved[i] = true
			} else if errors.Is(errs[localIdx], ErrDeleted) {
				resolved[i] = true
			}
		}

		keyIdx = batchEnd
		startIdx = sstIdx
	}

	// Map sorted results back to original input order
	result := make([][]byte, n)
	for i := range n {
		result[indices[i]] = sortedDst[i]
	}
	return result, nil
}

// getLocked performs a Get without acquiring the lock. Caller must hold flushMutex.
func (e *Engine) getLocked(key Key) ([]byte, error) {
	value, err := e.memtable.Get(key)

	if errors.Is(err, ErrNotFound) {
		// maybe another sst has it, continue
	} else if errors.Is(err, ErrDeleted) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return value, nil
	}

	for _, s := range e.l0ssts {
		value, err := e.GetInSST(s, key)

		if errors.Is(err, ErrNotFound) {
			continue
		} else if errors.Is(err, ErrDeleted) {
			return nil, nil
		} else if err != nil {
			return nil, err
		} else {
			return value, nil
		}
	}

	// Binary search on L1 SSTs
	index := sort.Search(len(e.l1ssts), func(i int) bool {
		return bytes.Compare(e.l1ssts[i].firstKey, key) > 0
	}) - 1

	if index < 0 || index >= len(e.l1ssts) {
		return nil, nil
	}

	value, err = e.GetInSST(e.l1ssts[index], key)

	if errors.Is(err, ErrNotFound) {
		return nil, nil
	} else if errors.Is(err, ErrDeleted) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return value, nil
	}
}

type mergeEntry struct {
	current KeyValue
	index   int
	source  Iterator
}

type mergeIterator struct {
	heap    Heap[mergeEntry]
	current KeyValue
	started bool
	sources []Iterator
}

func newMergeIterator(sources []Iterator) *mergeIterator {
	heap := newHeap(func(a mergeEntry, b mergeEntry) bool {
		cmp := bytes.Compare(a.current.Key, b.current.Key)
		if cmp != 0 {
			return cmp < 0
		}
		return a.index < b.index // lower index = newer source wins
	})

	for idx, source := range sources {
		if source.Next() {
			heap.Push(mergeEntry{
				current: source.Current(),
				index:   idx,
				source:  source,
			})
		}
	}

	return &mergeIterator{heap: heap, sources: sources}
}

func (it *mergeIterator) Next() bool {
	for it.heap.Len() > 0 {
		entry := it.heap.Pop()

		// Advance this source and re-push if it has more
		if entry.source.Next() {
			it.heap.Push(mergeEntry{
				current: entry.source.Current(),
				index:   entry.index,
				source:  entry.source,
			})
		}

		// Skip duplicate keys — we already yielded from a newer source
		if it.started && bytes.Equal(entry.current.Key, it.current.Key) {
			continue
		}
		it.current = entry.current
		it.started = true

		return true
	}
	return false
}

func (it *mergeIterator) Current() KeyValue {
	return it.current
}

func (it *mergeIterator) Close() error {
	var err error

	for _, source := range it.sources {
		err = source.Close()
	}

	return err
}

func (e *Engine) Scan(lo, hi Key) Iterator {
	// Clone mutates COW flags, so it needs an exclusive lock. It's O(1).
	e.flushMutex.Lock()
	memtable := e.memtable.Clone()
	l0ssts := make([]*sst, len(e.l0ssts))
	l1ssts := make([]*sst, len(e.l1ssts))
	copy(l0ssts, e.l0ssts)
	copy(l1ssts, e.l1ssts)
	e.flushMutex.Unlock()

	return e.scan(lo, hi, memtable, l0ssts, l1ssts)
}

func (e *Engine) scan(lo Key, hi Key, memtable *memtable, l0ssts []*sst, l1ssts []*sst) Iterator {
	var sources []Iterator

	if memtable != nil && memtable.Len() > 0 {
		sources = []Iterator{memtable.Scan(lo, hi)}
	}

	for _, s := range l0ssts {
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			continue
		}
		sources = append(sources, s.Iterator(lo, hi, handle))
	}

	// L1 SSTs are non-overlapping and sorted, so we walk them as a single
	// concatenated source instead of pushing each into the merge heap.
	if len(l1ssts) == 1 {
		handle, err := e.fileTable.getOrOpen(l1ssts[0].path)
		if err != nil {
			return newMergeIterator(sources)
		}
		sources = append(sources, l1ssts[0].Iterator(lo, hi, handle))
	} else if len(l1ssts) > 1 {
		opener := func(s *sst) (reader, error) {
			return e.fileTable.getOrOpen(s.path)
		}
		sources = append(sources, newSSTConcatIterator(l1ssts, lo, hi, opener))
	}

	if len(sources) == 1 {
		// If there is only one source, return it. This is much faster than merging
		return sources[0]
	}

	return newMergeIterator(sources)
}

func (e *Engine) maybeFlushLocked() error {
	if e.memtable.ByteSize() <= uint64(e.policy.FlushThreshold) {
		return nil
	}
	if err := e.flushLocked(); err != nil {
		return err
	}
	// Auto-flush triggers a background compaction. Manual Flush() does not —
	// tests that inspect L0/L1 state immediately after Flush() rely on it
	// being a synchronous, side-effect-free checkpoint.
	if len(e.l0ssts) > e.policy.SoftCompactionThreshold {
		if e.compactionMutex.TryLock() {
			go func() {
				defer e.compactionMutex.Unlock()
				e.compactLocked()
			}()
		}
	}
	return nil
}

func (e *Engine) Flush() error {
	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()
	return e.flushLocked()
}

func (e *Engine) flushLocked() error {
	new_ssts, err := WriteSST(e.ObjectsPath(), e.memtable.Entries(), true)
	if err != nil {
		return err
	}

	// Commit SST renames (and, via metadata-after-data ordering, their content)
	// before the manifest references them.
	if len(new_ssts) > 0 {
		err := syncDir(e.ObjectsPath())
		if err != nil {
			return err
		}
	}

	newHashes := slices.Clone(e.manifest.L0Hashes())
	for _, s := range new_ssts {
		newHashes = slices.Insert(newHashes, 0, s.hash)
	}

	if err := e.manifest.Update(levelL0, newHashes); err != nil {
		return err
	}
	err = e.manifest.Sync()
	if err != nil {
		return err
	}

	e.l0ssts = append(new_ssts, e.l0ssts...)
	e.memtable = newMemtable()
	e.wal.Clear()
	e.flushCount++

	return nil
}

func (e *Engine) Compact() error {
	e.compactionMutex.Lock()
	defer e.compactionMutex.Unlock()
	return e.compactLocked()
}

func (e *Engine) compactLocked() error {
	e.flushMutex.Lock()
	l0ssts := make([]*sst, len(e.l0ssts))
	l1ssts := make([]*sst, len(e.l1ssts))
	copy(l0ssts, e.l0ssts)
	copy(l1ssts, e.l1ssts)
	e.flushMutex.Unlock()

	l0Entries := e.scan(nil, nil, nil, l0ssts, nil)

	var new_ssts []*sst

	if len(l1ssts) == 0 {
		// L1 is bottom: tombstones have nothing to shadow below, drop them.
		written_ssts, err := WriteSST(e.ObjectsPath(), l0Entries, false)
		if err != nil {
			return err
		}
		new_ssts = append(new_ssts, written_ssts...)
	} else {
		entries := make([]KeyValue, 0)
		l1Index := 0

		finishRange := func() error {
			// There is no overlap between l0 and l1, we can keep the old l1 sst
			if len(entries) == 0 {
				new_ssts = append(new_ssts, l1ssts[l1Index])
			} else {
				handle, err := e.fileTable.getOrOpen(l1ssts[l1Index].path)
				if err != nil {
					return err
				}
				l0Range := iter(entries)
				l1Range := l1ssts[l1Index].Iterator(nil, nil, handle)
				// Merge preserves tombstones so newer L0 deletions shadow older
				// L1 values; WriteSST(false) drops them at the bottom level.
				mergedRange := newMergeIterator([]Iterator{l0Range, l1Range})
				rangeSsts, err := WriteSST(e.ObjectsPath(), mergedRange, false)
				handle.Close()
				if err != nil {
					return err
				}
				new_ssts = append(new_ssts, rangeSsts...)
			}

			entries = entries[:0]
			l1Index++
			return nil
		}

		for l0Entries.Next() {
			entry := l0Entries.Current()
			key := entry.Key
			value := entry.Value

			for l1Index+1 < len(l1ssts) && bytes.Compare(key, l1ssts[l1Index+1].firstKey) >= 0 {
				err := finishRange()
				if err != nil {
					return err
				}
			}

			entries = append(entries, KeyValue{Key: key, Value: value})
		}

		for l1Index < len(l1ssts) {
			err := finishRange()
			if err != nil {
				return err
			}
		}
	}

	// Commit SST renames (and their content, via FS ordering) before the
	// manifests reference them.
	if err := syncDir(e.ObjectsPath()); err != nil {
		return err
	}

	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()

	new_l1_ssts := make([]string, len(new_ssts))
	for i, sst := range new_ssts {
		new_l1_ssts[i] = sst.hash
	}
	// Order matters: L1 frame first, then L0. On a torn-tail crash the safe
	// outcome is "only L1 landed" — reads from L0 still shadow L1 correctly
	// with the same values. "L0 without L1" cannot happen because fd writes
	// preserve offset order.
	if err := e.manifest.Update(levelL1, new_l1_ssts); err != nil {
		return err
	}

	e.l1ssts = new_ssts

	newL0 := slices.Clone(e.manifest.L0Hashes())
	newL0 = newL0[:len(newL0)-len(l0ssts)]
	if err := e.manifest.Update(levelL0, newL0); err != nil {
		return err
	}
	e.l0ssts = e.l0ssts[:len(e.l0ssts)-len(l0ssts)]

	if err := e.manifest.Sync(); err != nil {
		return err
	}

	// Delete orphaned ssts
	kept := func(hash string) bool {
		for _, s := range new_ssts {
			if s.hash == hash {
				return true
			}
		}
		return false
	}
	for _, oldSST := range l1ssts {
		if !kept(oldSST.hash) {
			_ = os.Remove(oldSST.path)
		}
	}
	for _, oldSST := range l0ssts {
		if !kept(oldSST.hash) {
			_ = os.Remove(oldSST.path)
		}
	}

	e.compactionCount++

	return nil
}

func (e *Engine) CloudSync() error {
	// TODO
	return nil
}

// isSSTHash reports whether name is a 64-char lowercase hex string — the
// canonical SST filename format produced by WriteSST. This filter keeps GC
// from touching in-flight "-temp-XYZ" files created by os.CreateTemp.
func isSSTHash(name string) bool {
	if len(name) != 64 {
		return false
	}
	_, err := hex.DecodeString(name)
	return err == nil
}

// gcLocked removes SST files in ObjectsPath() that are not referenced by
// the manifest. Caller must hold compactionMutex and flushMutex.
func (e *Engine) gcLocked() error {
	l0 := e.manifest.L0Hashes()
	l1 := e.manifest.L1Hashes()
	referenced := make(map[string]struct{}, len(l0)+len(l1))
	for _, h := range l0 {
		referenced[h] = struct{}{}
	}
	for _, h := range l1 {
		referenced[h] = struct{}{}
	}

	dirEntries, err := os.ReadDir(e.ObjectsPath())
	if err != nil {
		return err
	}

	for _, entry := range dirEntries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if !isSSTHash(name) {
			continue
		}
		if _, ok := referenced[name]; ok {
			continue
		}

		// POSIX unlink keeps open fds valid, so active Scan iterators that
		// already hold a handle to this file continue to read successfully.
		// The fileTable's LRU entry is left to age out naturally — nothing
		// will look this path up again since it's out of all manifests.
		fullPath := filepath.Join(e.ObjectsPath(), name)
		err := os.Remove(fullPath)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

type txEntry struct {
	key   Key
	value []byte
}

type Transaction struct {
	engine   *Engine
	entries  []txEntry
	byteSize int
}

func (e *Engine) Transaction() Transaction {
	// Write stall: if L0 is too tall, force a synchronous compaction before
	// accepting the write. compactionMutex serializes so at most one writer
	// actually runs the compaction; others wait here and re-check after.
	if len(e.l0ssts) >= e.policy.HardCompactionThreshold {
		e.compactionMutex.Lock()
		if len(e.l0ssts) >= e.policy.HardCompactionThreshold {
			_ = e.compactLocked()
		}
		e.compactionMutex.Unlock()
	}

	e.flushMutex.Lock()
	return Transaction{
		engine: e,
	}
}

func (tx *Transaction) Put(key Key, value []byte) {
	tx.entries = append(tx.entries, txEntry{key: key, value: value})
	tx.byteSize += len(key) + len(value)
}

func (tx *Transaction) Delete(key Key) {
	tx.entries = append(tx.entries, txEntry{key: key, value: nil})
	tx.byteSize += len(key)
}

func (tx *Transaction) Get(key Key) ([]byte, error) {
	// Scan backwards — last write wins
	for idx := len(tx.entries) - 1; idx >= 0; idx-- {
		if bytes.Equal(tx.entries[idx].key, key) {
			if tx.entries[idx].value == nil {
				return nil, nil // deleted
			}
			return tx.entries[idx].value, nil
		}
	}
	return tx.engine.getLocked(key)
}

func (tx *Transaction) Commit() error {
	defer tx.engine.flushMutex.Unlock()

	batch := tx.engine.wal.Batch()
	for _, entry := range tx.entries {
		if entry.value == nil {
			batch.Delete(entry.key)
		} else {
			batch.Put(entry.key, entry.value)
		}
	}
	err := batch.Commit()
	if err != nil {
		return err
	}

	// WAL is durable — only now is it safe to make the writes visible.
	for _, entry := range tx.entries {
		if entry.value == nil {
			tx.engine.memtable.Delete(entry.key)
		} else {
			tx.engine.memtable.Put(entry.key, entry.value)
		}
	}

	return tx.engine.maybeFlushLocked()
}

func (tx *Transaction) Cancel() {
	tx.engine.flushMutex.Unlock()
}
