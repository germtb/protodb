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
	"sync/atomic"
)

type sstSlice []*sst

func (e *Engine) L0SSTs() []*sst { return []*sst(*e.l0ssts.Load()) }
func (e *Engine) L1SSTs() []*sst { return []*sst(*e.l1ssts.Load()) }

type Policy struct {
	SoftCompactionThreshold int
	HardCompactionThreshold int
	FlushThreshold          int
	Sync                    bool
}

type commitNode struct {
	tx   *Transaction
	next atomic.Pointer[commitNode]
}

type Engine struct {
	flushMutex      sync.RWMutex
	compactionMutex sync.RWMutex
	// seqnum tracks the "epoch" in the memtatable entries. It is used to allow
	// lock-free scans while other operations are happening.
	seqnum      atomic.Uint64
	memtable    atomic.Pointer[memtable]
	fs          FS
	path        string
	wal         *WAL
	manifest    *Manifest
	l0ssts      atomic.Pointer[sstSlice]
	l1ssts      atomic.Pointer[sstSlice]
	policy      *Policy
	commitHead  atomic.Pointer[commitNode]
	commitTail  atomic.Pointer[commitNode]
	commitMutex sync.Mutex
	fileTable   *FileTable
	blockCache  *blockCache

	// Counters (updated under flushMutex / compactionMutex respectively).
	flushCount      uint64
	compactionCount uint64
}

const (
	DefaultBlockCacheSize  int64 = 128 << 20 // 128 MB
	DefaultMaxOpenSSTFiles int   = 128
)

type EngineOption func(*engineConfig)

type engineConfig struct {
	fs              FS
	blockCacheSize  int64
	maxOpenSSTFiles int
}

// WithFS overrides the filesystem used by the engine. Defaults to DefaultFS
// (os-backed). Primarily for tests / error injection.
func WithFS(fs FS) EngineOption {
	return func(c *engineConfig) { c.fs = fs }
}

// WithBlockCacheSize sets the byte cap on the block-level LRU cache.
// Defaults to 128 MB.
func WithBlockCacheSize(bytes int64) EngineOption {
	return func(c *engineConfig) { c.blockCacheSize = bytes }
}

// WithMaxOpenSSTFiles sets the file-handle LRU capacity. Defaults to 128.
func WithMaxOpenSSTFiles(n int) EngineOption {
	return func(c *engineConfig) { c.maxOpenSSTFiles = n }
}

func loadSSTs(fs FS, objectsPath string, metadata []LevelMetadata, cache *blockCache) ([]*sst, error) {
	ssts := make([]*sst, 0, len(metadata))
	for _, m := range metadata {
		s, err := ReadSST(fs, objectsPath, m, nil, cache)
		if err != nil {
			return nil, err
		}
		ssts = append(ssts, s)
	}
	return ssts, nil
}

func Open(path string, options ...EngineOption) (*Engine, error) {
	config := engineConfig{
		fs:              DefaultFS,
		blockCacheSize:  DefaultBlockCacheSize,
		maxOpenSSTFiles: DefaultMaxOpenSSTFiles,
	}
	for _, opt := range options {
		opt(&config)
	}

	path = filepath.Join(path, "protodb")

	err := config.fs.MkdirAll(filepath.Join(path, "objects"), 0755)
	if err != nil {
		return nil, err
	}

	wal, err := newWAL(config.fs, filepath.Join(path, "wal"))
	if err != nil {
		return nil, err
	}

	manifest, err := newManifest(config.fs, filepath.Join(path, "manifest"))
	if err != nil {
		return nil, err
	}

	blockCache := newBlockCache(config.blockCacheSize)

	objectsPath := filepath.Join(path, "objects")
	l0ssts, err := loadSSTs(config.fs, objectsPath, manifest.L0(), blockCache)
	if err != nil {
		return nil, err
	}
	l1ssts, err := loadSSTs(config.fs, objectsPath, manifest.L1(), blockCache)
	if err != nil {
		return nil, err
	}

	memtable := newMemtable()
	_, err = wal.replay(memtable)

	if err != nil {
		return nil, err
	}

	sentinel := &commitNode{}

	e := &Engine{
		fs:         config.fs,
		path:       path,
		wal:        wal,
		manifest:   manifest,
		fileTable:  newFileTable(config.fs, config.maxOpenSSTFiles),
		blockCache: blockCache,
		policy: &Policy{
			SoftCompactionThreshold: 4,                // 4 L0 ssts
			HardCompactionThreshold: 16,               // 16 L0 ssts
			FlushThreshold:          1024 * 1024 * 64, // 64Mb
		},
	}

	e.commitHead.Store(sentinel)
	e.commitTail.Store(sentinel)
	e.seqnum.Store(0)

	e.memtable.Store(memtable)
	l0 := sstSlice(l0ssts)
	l1 := sstSlice(l1ssts)
	e.l0ssts.Store(&l0)
	e.l1ssts.Store(&l1)

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
	l0ssts := *e.l0ssts.Load()
	l1ssts := *e.l1ssts.Load()
	var l0Bytes, l1Bytes int64
	for _, s := range l0ssts {
		l0Bytes += s.fileSize
	}
	for _, s := range l1ssts {
		l1Bytes += s.fileSize
	}
	return EngineStats{
		FlushCount:      e.flushCount,
		CompactionCount: e.compactionCount,
		L0SSTs:          len(l0ssts),
		L0Bytes:         l0Bytes,
		L1SSTs:          len(l1ssts),
		L1Bytes:         l1Bytes,
	}
}

func (e *Engine) Close() error {
	e.compactionMutex.Lock()
	defer e.compactionMutex.Unlock()
	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()
	e.fileTable.Clear()
	e.blockCache.Close()
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
	return e.getLocked(key)
}

// BulkGet retrieves multiple keys in a single operation.
func (e *Engine) BulkGet(keys []Key) ([][]byte, error) {
	n := len(keys)
	if n == 0 {
		return nil, nil
	}

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

	seqnum := e.seqnum.Load()
	memtable := e.memtable.Load()
	l0ssts := *e.l0ssts.Load()
	l1ssts := *e.l1ssts.Load()

	// 1. Memtable (highest precedence)
	for i := range n {
		value, err := memtable.Get(sortedKeys[i], seqnum)
		if err == nil {
			sortedDst[i] = value
			resolved[i] = true
		} else if errors.Is(err, ErrDeleted) {
			resolved[i] = true // tombstone — don't fall through
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	// 2. L0 SSTs (newest first — captured above)
	for _, s := range l0ssts {
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

	// 3. L1 SSTs (sorted, non-overlapping; captured above). Bucket remaining
	// keys by SST.
	startIdx := 0
	for keyIdx := 0; keyIdx < n; {
		if resolved[keyIdx] {
			keyIdx++
			continue
		}

		k := sortedKeys[keyIdx]

		// Binary search [startIdx:] for the SST containing k
		ssts := l1ssts[startIdx:]
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
		if sstIdx+1 < len(l1ssts) {
			nextFirstKey = l1ssts[sstIdx+1].firstKey
		}

		batchEnd := keyIdx + 1
		for batchEnd < n {
			if nextFirstKey != nil && bytes.Compare(sortedKeys[batchEnd], nextFirstKey) >= 0 {
				break
			}
			batchEnd++
		}

		s := l1ssts[sstIdx]
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
	seqnum := e.seqnum.Load()
	value, err := e.memtable.Load().Get(key, seqnum)

	if errors.Is(err, ErrNotFound) {
		// maybe another sst has it, continue
	} else if errors.Is(err, ErrDeleted) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		return value, nil
	}

	l0ssts := *e.l0ssts.Load()
	for _, s := range l0ssts {
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
	l1ssts := *e.l1ssts.Load()
	index := sort.Search(len(l1ssts), func(i int) bool {
		return bytes.Compare(l1ssts[i].firstKey, key) > 0
	}) - 1

	if index < 0 || index >= len(l1ssts) {
		return nil, nil
	}

	value, err = e.GetInSST(l1ssts[index], key)

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

func newMergeIterator(sources []Iterator, reverse bool) *mergeIterator {
	heap := newHeap(func(a mergeEntry, b mergeEntry) bool {
		cmp := bytes.Compare(a.current.Key, b.current.Key)
		if reverse {
			cmp = -cmp
		}
		if cmp != 0 {
			return cmp < 0
		}
		return a.index < b.index
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

		// Skip duplicate user_keys — the first pop from the lowest-index
		// (newest) source already yielded this key.
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

func (e *Engine) Scan(lo Key, hi Key) Iterator {
	return e.scanImpl(lo, hi, false)
}

func (e *Engine) ReverseScan(lo Key, hi Key) Iterator {
	return e.scanImpl(lo, hi, true)
}

func (e *Engine) scanImpl(lo Key, hi Key, reverse bool) Iterator {
	seqnum := e.seqnum.Load()
	activeMemtable := e.memtable.Load()
	l0ssts := *e.l0ssts.Load()
	l1ssts := *e.l1ssts.Load()

	var sources []Iterator

	if activeMemtable != nil && activeMemtable.Len() > 0 {
		if reverse {
			sources = append(sources, activeMemtable.ReverseScan(lo, hi, seqnum))
		} else {
			sources = append(sources, activeMemtable.Scan(lo, hi, seqnum))
		}
	}

	scanIter := e.scan(lo, hi, nil, l0ssts, l1ssts, reverse)
	sources = append(sources, scanIter)

	if len(sources) == 1 {
		return skipTombstones(sources[0])
	}

	return skipTombstones(newMergeIterator(sources, reverse))
}

// builds an iterator across memtable + L0 + L1
func (e *Engine) scan(lo Key, hi Key, memtable *memtable, l0ssts []*sst, l1ssts []*sst, reverse bool) Iterator {
	var sources []Iterator

	if memtable != nil && memtable.Len() > 0 {
		sources = []Iterator{memtable.Scan(lo, hi, VisibleAll)}
	}

	for _, s := range l0ssts {
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			continue
		}
		sources = append(sources, s.Iterator(lo, hi, handle, reverse))
	}

	// L1 SSTs are non-overlapping and sorted, so we walk them as a single
	// concatenated source instead of pushing each into the merge heap.
	if len(l1ssts) == 1 {
		handle, err := e.fileTable.getOrOpen(l1ssts[0].path)
		if err != nil {
			return newMergeIterator(sources, reverse)
		}
		sources = append(sources, l1ssts[0].Iterator(lo, hi, handle, reverse))
	} else if len(l1ssts) > 1 {
		opener := func(s *sst) (reader, error) {
			return e.fileTable.getOrOpen(s.path)
		}
		sources = append(sources, newSSTConcatIterator(l1ssts, lo, hi, opener, reverse))
	}

	if len(sources) == 1 {
		// If there is only one source, return it. This is much faster than merging
		return sources[0]
	}

	return newMergeIterator(sources, reverse)
}

func (e *Engine) maybeFlush() error {
	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()
	if e.memtable.Load().ByteSize() <= uint64(e.policy.FlushThreshold) {
		return nil
	}
	err := e.flushLocked()
	if err != nil {
		return err
	}
	// Auto-flush triggers a background compaction. Manual Flush() does not —
	// tests that inspect L0/L1 state immediately after Flush() rely on it
	// being a synchronous, side-effect-free checkpoint.
	if soft := e.policy.SoftCompactionThreshold; soft > 0 && len(*e.l0ssts.Load()) > soft {
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
	e.commitMutex.Lock()
	e.flushMutex.Lock()
	err := e.flushLocked()
	e.flushMutex.Unlock()
	e.commitMutex.Unlock()

	// It is possible that flush stopped a commit (since they use TryLock on
	// the commitMutex) so we kick the commit loop just in case.
	_ = e.commitLoop(0)

	return err
}

func (e *Engine) flushLocked() error {
	// Capture the seqnum boundary
	flushUpTo := e.seqnum.Load()
	memtable := e.memtable.Load()

	new_ssts, err := WriteSST(e.fs, e.ObjectsPath(), memtable.Scan(nil, nil, flushUpTo), true, e.blockCache)
	if err != nil {
		return err
	}

	// Commit SST renames (and, via metadata-after-data ordering, their content)
	// before the manifest references them.
	if len(new_ssts) > 0 {
		err := syncDir(e.fs, e.ObjectsPath())
		if err != nil {
			return err
		}
	}

	newL0Meta := slices.Clone(e.manifest.L0())
	for _, s := range new_ssts {
		newL0Meta = slices.Insert(newL0Meta, 0, LevelMetadata{hash: s.hash, first: s.firstKey, last: s.lastKey})
	}

	if err := e.manifest.Update(levelL0, newL0Meta); err != nil {
		return err
	}
	err = e.manifest.Sync()
	if err != nil {
		return err
	}

	// Update L0
	newL0 := sstSlice(append(new_ssts, *e.l0ssts.Load()...))
	e.l0ssts.Store(&newL0)
	// Now we can update L1
	e.memtable.Store(newMemtable())

	if err := e.wal.Clear(); err != nil {
		return err
	}
	e.flushCount++

	return nil
}

func (e *Engine) Compact() error {
	e.compactionMutex.Lock()
	defer e.compactionMutex.Unlock()
	return e.compactLocked()
}

func (e *Engine) compactLocked() error {
	l0ssts := slices.Clone([]*sst(*e.l0ssts.Load()))
	l1ssts := slices.Clone([]*sst(*e.l1ssts.Load()))

	originalL1ssts := slices.Clone(l1ssts)

	anyRewrite := false

	for idx := len(l0ssts) - 1; idx >= 0; idx-- {
		l0 := l0ssts[idx]

		// L1 is sorted + non-overlapping → overlapping range is contiguous.
		first, last := -1, -1
		for i, l1 := range l1ssts {
			if bytes.Compare(l1.firstKey, l0.lastKey) > 0 {
				break // l1 has passed l0
			}
			if l1.lastKey != nil && bytes.Compare(l1.lastKey, l0.firstKey) < 0 {
				continue // l1 still before l0
			}
			if first < 0 {
				first = i
			}
			last = i
		}

		if first < 0 {
			// No overlap — find insertion position and splice l0 in.
			pos := sort.Search(len(l1ssts), func(i int) bool {
				return bytes.Compare(l1ssts[i].firstKey, l0.firstKey) > 0
			})
			l1ssts = slices.Insert(l1ssts, pos, l0)
			continue
		}

		// Merge l0 with all of l1ssts[first:last+1]. The merge reads every
		// block of every input exactly once; those blocks land in the
		// engine's block cache and may evict hot foreground entries.
		// We accept that — bounding cache usage is the operator's lever
		// (WithBlockCacheSize), not a per-call flag.
		merged := e.scan(nil, nil, nil, []*sst{l0}, l1ssts[first:last+1], false)
		written, err := WriteSST(e.fs, e.ObjectsPath(), merged, false, e.blockCache)
		merged.Close()
		if err != nil {
			return err
		}
		anyRewrite = true
		l1ssts = slices.Replace(l1ssts, first, last+1, written...)
	}

	new_ssts := l1ssts

	// Only sync when there has been a write
	if anyRewrite {
		err := syncDir(e.fs, e.ObjectsPath())
		if err != nil {
			return err
		}
	}

	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()

	newL1Meta := make([]LevelMetadata, len(new_ssts))
	for i, sst := range new_ssts {
		newL1Meta[i] = LevelMetadata{hash: sst.hash, first: sst.firstKey, last: sst.lastKey}
	}
	// Order matters: L1 frame first, then L0. On a torn-tail crash the safe
	// outcome is "only L1 landed" — reads from L0 still shadow L1 correctly
	// with the same values. "L0 without L1" cannot happen because fd writes
	// preserve offset order.
	err := e.manifest.Update(levelL1, newL1Meta)
	if err != nil {
		return err
	}

	newL1 := sstSlice(new_ssts)
	e.l1ssts.Store(&newL1)

	newL0Meta := slices.Clone(e.manifest.L0())
	newL0Meta = newL0Meta[:len(newL0Meta)-len(l0ssts)]
	if err := e.manifest.Update(levelL0, newL0Meta); err != nil {
		return err
	}
	currentL0 := *e.l0ssts.Load()
	trimmedL0 := sstSlice(currentL0[:len(currentL0)-len(l0ssts)])
	e.l0ssts.Store(&trimmedL0)

	if err := e.manifest.Sync(); err != nil {
		return err
	}

	kept := func(hash sstHash) bool {
		for _, s := range new_ssts {
			if s.hash == hash {
				return true
			}
		}
		return false
	}
	for _, oldSST := range originalL1ssts {
		if !kept(oldSST.hash) {
			_ = e.fs.Remove(oldSST.path)
		}
	}
	for _, oldSST := range l0ssts {
		if !kept(oldSST.hash) {
			_ = e.fs.Remove(oldSST.path)
		}
	}

	e.compactionCount++

	return nil
}

func (e *Engine) CloudSync() error {
	// TODO
	return nil
}

// parseSSTHash decodes a 64-char hex SST filename into its raw sstHash.
// Returns ok=false for anything that's not a canonical SST name (subdir,
// in-flight "-temp-XYZ" file, etc), keeping GC from touching them.
func parseSSTHash(name string) (sstHash, bool) {
	var h sstHash
	if len(name) != 64 {
		return h, false
	}
	if _, err := hex.Decode(h[:], []byte(name)); err != nil {
		return h, false
	}
	return h, true
}

// gcLocked removes SST files in ObjectsPath() that are not referenced by
// the manifest. Caller must hold compactionMutex and flushMutex.
func (e *Engine) gcLocked() error {
	l0 := e.manifest.L0()
	l1 := e.manifest.L1()
	referenced := make(map[sstHash]struct{}, len(l0)+len(l1))
	for _, m := range l0 {
		referenced[m.hash] = struct{}{}
	}
	for _, m := range l1 {
		referenced[m.hash] = struct{}{}
	}

	dirEntries, err := e.fs.List(e.ObjectsPath())
	if err != nil {
		return err
	}

	for _, name := range dirEntries {
		// Subdirs don't match the 64-char hex SST hash shape, so parseSSTHash
		// filters them out along with any in-flight "-temp-XYZ" names.
		h, ok := parseSSTHash(name)
		if !ok {
			continue
		}
		if _, ok := referenced[h]; ok {
			continue
		}

		// POSIX unlink keeps open fds valid, so active Scan iterators that
		// already hold a handle to this file continue to read successfully.
		// The fileTable's LRU entry is left to age out naturally — nothing
		// will look this path up again since it's out of all manifests.
		fullPath := filepath.Join(e.ObjectsPath(), name)
		err := e.fs.Remove(fullPath)
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
	done     chan error
}

// transactionPool reuses Transaction structs + their underlying entries
// slice capacity and done channel across commits. Transaction() always
// returns a pointer now (previously a value) so Put/Delete's append on
// tx.entries survives the pool round-trip.
//
// Safety: a pooled Transaction is only returned after Commit's queue
// receive, at which point commitLoop is no longer reading it — the
// happens-before from the done channel covers the handoff.
var transactionPool = sync.Pool{
	New: func() any {
		return &Transaction{done: make(chan error, 1)}
	},
}

func (e *Engine) Transaction() *Transaction {
	// Write stall: if L0 is too tall, force a synchronous compaction before
	// accepting the write. compactionMutex serializes so at most one writer
	// actually runs the compaction; others wait here and re-check after.
	// A zero/negative threshold disables the write stall entirely — without
	// this guard a partially-populated Policy (missing HardCompactionThreshold)
	// would fire on every Put, since 0 L0 SSTs satisfies `>= 0`.
	if hard := e.policy.HardCompactionThreshold; hard > 0 && len(*e.l0ssts.Load()) >= hard {
		e.compactionMutex.Lock()
		if len(*e.l0ssts.Load()) >= hard {
			_ = e.compactLocked()
		}
		e.compactionMutex.Unlock()
	}

	tx := transactionPool.Get().(*Transaction)
	tx.engine = e
	tx.entries = tx.entries[:0]
	tx.byteSize = 0
	return tx
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
	err := tx.engine.queueTransaction(tx)
	tx.engine = nil
	transactionPool.Put(tx)
	return err
}

func (tx *Transaction) Cancel() {
	tx.engine = nil
	transactionPool.Put(tx)
}

func (e *Engine) queueTransaction(tx *Transaction) error {
	node := &commitNode{tx: tx}

	for {
		tail := e.commitTail.Load()
		if tail.next.CompareAndSwap(nil, node) {
			e.commitTail.CompareAndSwap(tail, node)
			break
		}
		// tail.next is not nil — another enqueuer already linked here.
		// Reload commitTail on next iteration to get the latest.
	}

	e.commitLoop(0)

	return <-tx.done
}

const MAX_COMMIT_LOOP_STACK = 1000

type transactionResult struct {
	tx  *Transaction
	err error
}

func (e *Engine) commitLoop(counter int) error {
	if counter > MAX_COMMIT_LOOP_STACK {
		return nil
	}
	if !e.commitMutex.TryLock() {
		return nil
	}

	node := e.commitHead.Load().next.Load()
	tail := e.commitTail.Load()

	shouldSync := false

	txResults := make([]transactionResult, 0)

	for node != nil {
		tx := node.tx
		batch := node.tx.engine.wal.Batch()
		// Pre-size the batch buffer. Each frame adds 12 bytes of overhead
		// (crc + key_len + val_len) on top of key+value bytes; +8 for the
		// trailing commit marker. Close enough to skip the reallocation chain.
		batch.Grow(tx.byteSize + 12*len(tx.entries) + 8)
		for _, entry := range node.tx.entries {
			if entry.value == nil {
				batch.Delete(entry.key)
			} else {
				batch.Put(entry.key, entry.value)
			}
		}
		err := batch.Commit()

		if e.policy.Sync {
			txResults = append(txResults, transactionResult{tx, err})
		} else {
			node.tx.done <- err
		}

		if err == nil {
			shouldSync = true
			memtable := e.memtable.Load()
			startSeq := e.seqnum.Load() + 1
			localSeq := startSeq
			for _, entry := range node.tx.entries {
				if entry.value == nil {
					memtable.Delete(entry.key, localSeq)
				} else {
					memtable.Put(entry.key, entry.value, localSeq)
				}
				localSeq++
			}
			// Publish the tx atomically: Store only after every entry is in
			// the memtable, so readers never see a half-applied transaction.
			if localSeq > startSeq {
				e.seqnum.Store(localSeq - 1)
			}
		}

		e.commitHead.Store(node)

		if node == tail {
			// We already reached the tail, let's stop
			break
		}

		node = node.next.Load()
	}

	if e.policy.Sync {
		var err error
		if shouldSync {
			err = e.wal.sync()
		}

		for _, txResult := range txResults {
			if txResult.err == nil {
				txResult.tx.done <- err
			} else {
				txResult.tx.done <- txResult.err
			}
		}

		if err != nil {
			e.commitMutex.Unlock()
			return err
		}
	}

	if e.memtable.Load().ByteSize() > uint64(e.policy.FlushThreshold) {
		if err := e.maybeFlush(); err != nil {
			e.commitMutex.Unlock()
			return err
		}
	}

	e.commitMutex.Unlock()

	if node := e.commitHead.Load().next.Load(); node != nil {
		// New work arrived!
		return e.commitLoop(counter + 1)
	}

	return nil
}
