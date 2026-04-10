package protodb

import (
	"bytes"

	"github.com/google/btree"
)

type mementry struct {
	key   Key
	value []byte
}

type memtable struct {
	tree     *btree.BTreeG[mementry]
	byteSize uint64
}

func newMemtable() memtable {
	return memtable{
		tree: btree.NewG[mementry](32, func(a, b mementry) bool {
			return bytes.Compare(a.key, b.key) < 0
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

func (m *memtable) Put(key Key, value []byte) {
	old, replaced := m.tree.ReplaceOrInsert(mementry{key: key, value: value})
	if replaced {
		m.byteSize -= uint64(len(old.key)) + uint64(len(old.value))
	}
	m.byteSize += uint64(len(key)) + uint64(len(value))
}

func (m *memtable) Delete(key Key) {
	old, replaced := m.tree.ReplaceOrInsert(mementry{key: key, value: nil})
	if replaced {
		m.byteSize -= uint64(len(old.key)) + uint64(len(old.value))
	}
	m.byteSize += uint64(len(key))
}

func (m *memtable) Len() int {
	return m.tree.Len()
}

func (m *memtable) Get(key Key) ([]byte, error) {
	entry, found := m.tree.Get(mementry{key: key})

	if !found {
		return nil, ErrNotFound
	} else if entry.value == nil {
		return nil, ErrDeleted
	}

	return entry.value, nil
}

type memtableIterator struct {
	entries []mementry
	pos     int
}

func (it *memtableIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

func (it *memtableIterator) Key() Key {
	return it.entries[it.pos].key
}

func (it *memtableIterator) Value() []byte {
	return it.entries[it.pos].value
}

func (m *memtable) Scan(lo Key, hi Key) *memtableIterator {
	var entries []mementry
	m.tree.AscendRange(mementry{key: lo}, mementry{key: hi}, func(e mementry) bool {
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
