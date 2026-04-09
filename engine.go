package protodb

import (
	"bytes"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
)

type FileTable struct {
	mu  sync.Mutex
	lru *LRU[string, *os.File]
}

type Engine struct {
	flushMutex sync.RWMutex
	memtable   memtable
	ssts       []*sst
	path       string
	fileTable  *FileTable
	wal        *WAL
	manifest   *Manifest
}

func (ft *FileTable) getOrOpen(path string) (*os.File, error) {
	ft.mu.Lock()
	defer ft.mu.Unlock()

	handle, ok := ft.lru.Get(path)
	if ok {
		return handle, nil
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	ft.lru.Put(path, file)
	return file, nil
}

func (ft *FileTable) Read(path string, offset int64, length int64) ([]byte, error) {
	handle, err := ft.getOrOpen(path)
	if err != nil {
		return nil, err
	}

	buffer := make([]byte, length)
	if _, err := handle.ReadAt(buffer, offset); err != nil {
		return nil, err
	}
	return buffer, nil
}

func (ft *FileTable) Close(path string) {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.lru.Remove(path)
}

func (ft *FileTable) Clear() {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	ft.lru.Clear()
}

func newFileTable(capacity int) *FileTable {
	return &FileTable{
		lru: newLRU(capacity, func(path string, file *os.File) {
			file.Close()
		}),
	}
}

func Open(path string) (*Engine, error) {
	path = filepath.Join(path, "protodb")

	wal, err := newWAL(filepath.Join(path, "wal"))
	if err != nil {
		return nil, err
	}

	manifest, err := newManifest(filepath.Join(path, "manifest"))
	if err != nil {
		return nil, err
	}

	_, err = os.ReadDir(path)

	if errors.Is(err, os.ErrNotExist) {
		err = os.MkdirAll(filepath.Join(path, "objects"), 0755)
		if err != nil {
			return nil, err
		}

		return &Engine{
			ssts:      make([]*sst, 0),
			path:      path,
			memtable:  newMemtable(),
			fileTable: newFileTable(128),
			wal:       wal,
			manifest:  manifest,
		}, nil
	} else if err != nil {
		return nil, err
	}

	os.MkdirAll(filepath.Join(path, "objects"), 0755)

	ssts := make([]*sst, 0)

	memtable := newMemtable()
	err = wal.replay(&memtable)

	if err != nil {
		return nil, err
	}

	for _, entry := range manifest.hashes {
		sst, err := ReadSST(filepath.Join(path, "objects"), entry, nil)

		if err != nil {
			return nil, err
		}

		ssts = append(ssts, sst)
	}

	return &Engine{
		memtable:  memtable,
		ssts:      ssts,
		path:      path,
		fileTable: newFileTable(128),
		wal:       wal,
		manifest:  manifest,
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

func (e *Engine) Put(key uint64, value []byte) error {
	tx := e.Transaction()
	tx.Put(key, value)
	return tx.Apply()
}

func (e *Engine) Delete(key uint64) error {
	tx := e.Transaction()
	tx.Delete(key)
	return tx.Apply()
}

func (e *Engine) Get(key uint64) ([]byte, error) {
	e.flushMutex.RLock()
	defer e.flushMutex.RUnlock()

	value, err := e.memtable.Get(key)

	if err == nil {
		return value, nil
	}

	if errors.Is(err, ErrDeleted) {
		return nil, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	for _, s := range e.ssts {
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			return nil, err
		}

		value, err = s.Get(key, handle)

		if err == nil {
			return value, nil
		}

		if errors.Is(err, ErrDeleted) {
			return nil, nil
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
	}

	return nil, nil
}

type mergeEntry struct {
	key    uint64
	value  []byte
	index  int
	source Iterator
}

type mergeIterator struct {
	heap    Heap[mergeEntry]
	key     uint64
	value   []byte
	started bool
}

func newMergeIterator(sources []Iterator) *mergeIterator {
	heap := newHeap(func(a mergeEntry, b mergeEntry) bool {
		if a.key != b.key {
			return a.key < b.key
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
		if it.started && entry.key == it.key {
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

func (it *mergeIterator) Key() uint64 {
	return it.key
}

func (it *mergeIterator) Value() []byte {
	return it.value
}

func (e *Engine) Scan(lo, hi uint64) *mergeIterator {
	// Clone mutates COW flags, so it needs an exclusive lock. It's O(1).
	e.flushMutex.Lock()
	snapshot := e.memtable.Clone()
	ssts := make([]*sst, len(e.ssts))
	copy(ssts, e.ssts)
	e.flushMutex.Unlock()

	return e.scan(lo, hi, snapshot.Scan(lo, hi), ssts)
}

func (e *Engine) scan(lo, hi uint64, memSource Iterator, ssts []*sst) *mergeIterator {
	sources := []Iterator{memSource}
	for _, s := range ssts {
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			continue
		}
		sources = append(sources, s.Iterator(lo, hi, handle))
	}
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
	e.manifest.Prepend(newHashes)

	err = e.manifest.Save()
	if err != nil {
		return err
	}

	e.ssts = append(new_ssts, e.ssts...)
	e.memtable = newMemtable()
	e.wal.Clear()

	return nil
}

func (e *Engine) Compact() error {
	e.flushMutex.Lock()
	defer e.flushMutex.Unlock()

	entries := e.scan(0, math.MaxUint64, e.memtable.Scan(0, math.MaxUint64), e.ssts)

	new_ssts, err := WriteSST(e.ObjectsPath(), entries)

	if err != nil {
		return err
	}

	e.manifest.Clear()
	for _, sst := range new_ssts {
		e.manifest.Append(sst.hash)
	}

	err = e.manifest.Save()
	if err != nil {
		return err
	}

	e.fileTable.Clear()
	e.ssts = new_ssts
	e.memtable = newMemtable()
	err = e.wal.Clear()
	if err != nil {
		return err
	}

	return nil
}

func (e *Engine) Sync() error {
	return nil
}

type txEntry struct {
	key   uint64
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

func (tx *Transaction) Put(key uint64, value []byte) {
	tx.entries = append(tx.entries, txEntry{key: key, value: value})
	tx.byteSize += len(value)
}

func (tx *Transaction) Delete(key uint64) {
	tx.entries = append(tx.entries, txEntry{key: key, value: nil})
}

func (tx *Transaction) Get(key uint64) ([]byte, error) {
	// Scan backwards — last write wins
	for idx := len(tx.entries) - 1; idx >= 0; idx-- {
		if tx.entries[idx].key == key {
			if tx.entries[idx].value == nil {
				return nil, nil // deleted
			}
			return tx.entries[idx].value, nil
		}
	}
	return tx.engine.Get(key)
}

func (tx *Transaction) Apply() error {
	defer tx.engine.flushMutex.Unlock()

	var buf bytes.Buffer
	buf.Grow(tx.byteSize + len(tx.entries)*(walHeaderSize+walEntryFixedSize))

	for _, entry := range tx.entries {
		writeFrame(&buf, entry.key, entry.value)
		if entry.value == nil {
			tx.engine.memtable.Delete(entry.key)
		} else {
			tx.engine.memtable.Put(entry.key, entry.value)
		}
	}

	return tx.engine.wal.Write(buf.Bytes())
}

func (tx *Transaction) Cancel() {
	tx.engine.flushMutex.Unlock()
}
