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

	if err == nil {
		return value, nil
	}

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
			return value, err
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

func (e *Engine) Scan(lo, hi Key) *mergeIterator {
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

func (e *Engine) scan(lo, hi Key, memSource Iterator, l0ssts []*sst, l1ssts []*sst) *mergeIterator {
	sources := []Iterator{memSource}
	for _, s := range l0ssts {
		handle, err := e.fileTable.getOrOpen(s.path)
		if err != nil {
			continue
		}
		sources = append(sources, s.Iterator(lo, hi, handle))
	}

	for _, s := range l1ssts {
		if hi != nil && bytes.Compare(s.firstKey, hi) >= 0 {
			break
		}

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

type emptyIterator struct{}

func (it *emptyIterator) Next() bool    { return false }
func (it *emptyIterator) Key() Key      { return nil }
func (it *emptyIterator) Value() []byte { return nil }

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
		&emptyIterator{},
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
	return tx.engine.Get(key)
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
