package protodb

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

type Level struct {
	manifest *Manifest
	ssts     []*sst
}

type Engine struct {
	flushMutex sync.RWMutex
	memtable   memtable
	path       string
	fileTable  *FileTable
	wal        *WAL
	l0         *Level
	l1         *Level
}

func newLevel(path string, name string) (*Level, error) {
	manifest, err := newManifest(filepath.Join(path, name))
	if err != nil {
		return nil, err
	}

	ssts := make([]*sst, 0)

	for _, entry := range manifest.hashes {
		sst, err := ReadSST(filepath.Join(path, "objects"), entry, nil)

		if err != nil {
			return nil, err
		}

		ssts = append(ssts, sst)
	}

	return &Level{
		manifest,
		ssts,
	}, nil
}

type Options struct {
	WALBufferSize int
}

func Open(path string, opts ...Options) (*Engine, error) {
	path = filepath.Join(path, "protodb")

	walBufferSize := 0
	if len(opts) > 0 {
		walBufferSize = opts[0].WALBufferSize
	}

	err := os.MkdirAll(filepath.Join(path, "objects"), 0755)
	if err != nil {
		return nil, err
	}

	wal, err := newWAL(filepath.Join(path, "wal"), walBufferSize)
	if err != nil {
		return nil, err
	}

	l0, err := newLevel(path, "l0")
	if err != nil {
		return nil, err
	}

	l1, err := newLevel(path, "l1")
	if err != nil {
		return nil, err
	}

	memtable := newMemtable()
	err = wal.replay(&memtable)

	if err != nil {
		return nil, err
	}

	return &Engine{
		memtable:  memtable,
		path:      path,
		fileTable: newFileTable(128),
		wal:       wal,
		l0:        l0,
		l1:        l1,
	}, nil
}

func (e *Engine) WALPath() string {
	return filepath.Join(e.path, "wal")
}

func (e *Engine) ObjectsPath() string {
	return filepath.Join(e.path, "objects")
}

func (e *Engine) ObjectPath(hash string) string {
	return filepath.Join(e.ObjectsPath(), hash)
}

func (e *Engine) Close() error {
	e.fileTable.Clear()
	return e.wal.Close()
}

func (e *Engine) Put(key Key, value []byte) error {
	tx := e.Transaction()
	tx.Put(key, value)
	return tx.Apply()
}

func (e *Engine) Delete(key Key) error {
	tx := e.Transaction()
	tx.Delete(key)
	return tx.Apply()
}

func (e *Engine) GetInSST(s *sst, key Key) ([]byte, error) {
	handle, err := e.fileTable.getOrOpen(s.path)
	if err != nil {
		return nil, err
	}

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
	for _, s := range e.l0.ssts {
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			return nil, err
		}
		values, errs, err := s.BulkGet(sortedKeys, handle)
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
		ssts := e.l1.ssts[startIdx:]
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
		if sstIdx+1 < len(e.l1.ssts) {
			nextFirstKey = e.l1.ssts[sstIdx+1].firstKey
		}

		batchEnd := keyIdx + 1
		for batchEnd < n {
			if nextFirstKey != nil && bytes.Compare(sortedKeys[batchEnd], nextFirstKey) >= 0 {
				break
			}
			batchEnd++
		}

		s := e.l1.ssts[sstIdx]
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			return nil, err
		}
		values, errs, err := s.BulkGet(sortedKeys[keyIdx:batchEnd], handle)
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

	for _, s := range e.l0.ssts {
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
	index := sort.Search(len(e.l1.ssts), func(i int) bool {
		return bytes.Compare(e.l1.ssts[i].firstKey, key) > 0
	}) - 1

	if index < 0 || index >= len(e.l1.ssts) {
		return nil, nil
	}

	value, err = e.GetInSST(e.l1.ssts[index], key)

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
	key    Key
	value  []byte
	index  int
	source Iterator
}

type mergeIterator struct {
	heap    Heap[mergeEntry]
	key     Key
	value   []byte
	started bool
}

func newMergeIterator(sources []Iterator) *mergeIterator {
	heap := newHeap(func(a mergeEntry, b mergeEntry) bool {
		cmp := bytes.Compare(a.key, b.key)
		if cmp != 0 {
			return cmp < 0
		}
		return a.index < b.index // lower index = newer source wins
	})

	for idx, source := range sources {
		if source.Next() {
			heap.Push(mergeEntry{
				key:    source.Key(),
				value:  source.Value(),
				index:  idx,
				source: source,
			})
		}
	}

	return &mergeIterator{heap: heap}
}

func (it *mergeIterator) Next() bool {
	for it.heap.Len() > 0 {
		entry := it.heap.Pop()

		// Advance this source and re-push if it has more
		if entry.source.Next() {
			it.heap.Push(mergeEntry{
				key:    entry.source.Key(),
				value:  entry.source.Value(),
				index:  entry.index,
				source: entry.source,
			})
		}

		// Skip duplicate keys — we already yielded from a newer source
		if it.started && bytes.Equal(entry.key, it.key) {
			continue
		}
		it.key = entry.key
		it.value = entry.value
		it.started = true

		// Skip tombstones — user shouldn't see them
		if entry.value == nil {
			continue
		}

		return true
	}
	return false
}

func (it *mergeIterator) Key() Key {
	return it.key
}

func (it *mergeIterator) Value() []byte {
	return it.value
}

func (e *Engine) Scan(lo, hi Key) Iterator {
	// Clone mutates COW flags, so it needs an exclusive lock. It's O(1).
	e.flushMutex.Lock()
	snapshot := e.memtable.Clone()
	l0ssts := make([]*sst, len(e.l0.ssts))
	l1ssts := make([]*sst, len(e.l1.ssts))
	copy(l0ssts, e.l0.ssts)
	copy(l1ssts, e.l1.ssts)
	e.flushMutex.Unlock()

	return e.scan(lo, hi, snapshot.Scan(lo, hi), l0ssts, l1ssts)
}

func (e *Engine) scan(lo, hi Key, memSource Iterator, l0ssts []*sst, l1ssts []*sst) Iterator {
	var sources []Iterator
	if memSource != nil {
		sources = []Iterator{memSource}
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
	if len(l1ssts) > 0 {
		opener := func(s *sst) (reader, error) {
			return e.fileTable.getOrOpen(s.path)
		}
		sources = append(sources, newSSTConcatIterator(l1ssts, lo, hi, opener))
	}

	if len(sources) == 1 {
		// If there is only one source, return it. This is much faster than merging
		return sources[0]
	}

	// Merge all the sources
	return newMergeIterator(sources)
}

func (e *Engine) Flush() error {
	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()

	new_ssts, err := WriteSST(e.ObjectsPath(), e.memtable.Entries())

	if err != nil {
		return err
	}

	var newHashes []string
	for _, sst := range new_ssts {
		newHashes = append(newHashes, sst.hash)
	}
	e.l0.manifest.Prepend(newHashes)

	err = e.l0.manifest.Save()
	if err != nil {
		return err
	}

	e.l0.ssts = append(new_ssts, e.l0.ssts...)
	e.memtable = newMemtable()
	e.wal.Clear()

	return nil
}

func (e *Engine) Compact() error {
	e.flushMutex.Lock()

	l0ssts := make([]*sst, len(e.l0.ssts))
	l1ssts := make([]*sst, len(e.l1.ssts))
	copy(l0ssts, e.l0.ssts)
	copy(l1ssts, e.l1.ssts)

	e.flushMutex.Unlock()

	entries := e.scan(
		nil,
		nil,
		nil,
		l0ssts,
		l1ssts,
	)

	new_ssts, err := WriteSST(e.ObjectsPath(), entries)

	if err != nil {
		return err
	}

	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()

	e.l1.manifest.Clear()
	for _, sst := range new_ssts {
		e.l1.manifest.Append(sst.hash)
	}

	err = e.l1.manifest.Save()
	if err != nil {
		return err
	}

	e.l1.ssts = new_ssts

	e.l0.ssts = e.l0.ssts[:len(e.l0.ssts)-len(l0ssts)]
	e.l0.manifest.TrimEnd(len(l0ssts))
	err = e.l0.manifest.Save()
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) Sync() error {
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

func (tx *Transaction) Apply() error {
	defer tx.engine.flushMutex.Unlock()

	batch := tx.engine.wal.Batch()

	for _, entry := range tx.entries {
		if entry.value == nil {
			batch.Delete(entry.key)
			tx.engine.memtable.Delete(entry.key)
		} else {
			batch.Put(entry.key, entry.value)
			tx.engine.memtable.Put(entry.key, entry.value)
		}
	}

	return batch.Commit()
}

func (tx *Transaction) Cancel() {
	tx.engine.flushMutex.Unlock()
}
