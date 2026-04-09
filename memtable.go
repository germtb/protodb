package protodb

import (
	"github.com/google/btree"
)

type mementry struct {
	Key   uint64
	Value []byte
}

type memtable struct {
	tree     *btree.BTreeG[mementry]
	byteSize uint64
}

func newMemtable() memtable {
	return memtable{
		tree: btree.NewG[mementry](32, func(a, b mementry) bool {
			return a.Key < b.Key
		}),
		byteSize: 0,
	}
}

func (m *memtable) Clone() memtable {
	return memtable{
		tree:     m.tree.Clone(),
		byteSize: m.byteSize,
	}
}

func (m *memtable) Put(key uint64, value []byte) {
	old, replaced := m.tree.ReplaceOrInsert(mementry{Key: key, Value: value})
	if replaced {
		m.byteSize -= uint64(len(old.Value))
	}
	m.byteSize += uint64(len(value))
}

func (m *memtable) Delete(key uint64) {
	old, replaced := m.tree.ReplaceOrInsert(mementry{Key: key, Value: nil})
	if replaced {
		m.byteSize -= uint64(len(old.Value))
	}
}

func (m *memtable) Len() int {
	return m.tree.Len()
}

func (m *memtable) Get(key uint64) ([]byte, error) {
	entry, found := m.tree.Get(mementry{Key: key})

	if !found {
		return nil, ErrNotFound
	} else if entry.Value == nil {
		return nil, ErrDeleted
	}

	return entry.Value, nil
}

type memtableIterator struct {
	entries []mementry
	pos     int
}

func (it *memtableIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

func (it *memtableIterator) Key() uint64 {
	return it.entries[it.pos].Key
}

func (it *memtableIterator) Value() []byte {
	return it.entries[it.pos].Value
}

func (m *memtable) Scan(lo uint64, hi uint64) *memtableIterator {
	var entries []mementry
	m.tree.AscendRange(mementry{Key: lo}, mementry{Key: hi}, func(e mementry) bool {
		entries = append(entries, e)
		return true
	})
	return &memtableIterator{entries: entries, pos: -1}
}

func (m *memtable) ByteSize() uint64 {
	return m.byteSize
}

func (m *memtable) Entries() *memtableIterator {
	var entries []mementry
	m.tree.Ascend(func(e mementry) bool {
		entries = append(entries, e)
		return true
	})
	return &memtableIterator{entries: entries, pos: -1}
}
