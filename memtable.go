package protodb

import (
	"bytes"

	"github.com/google/btree"
)

type memtable struct {
	tree     *btree.BTreeG[KeyValue]
	byteSize uint64
}

func newMemtable() *memtable {
	return &memtable{
		tree: btree.NewG(32, func(a, b KeyValue) bool {
			return bytes.Compare(a.Key, b.Key) < 0
		}),
		byteSize: 0,
	}
}

func (m *memtable) Clone() *memtable {
	return &memtable{
		tree:     m.tree.Clone(),
		byteSize: m.byteSize,
	}
}

func (m *memtable) Put(key Key, value []byte) {
	old, replaced := m.tree.ReplaceOrInsert(KeyValue{Key: key, Value: value})
	if replaced {
		m.byteSize -= uint64(len(old.Key)) + uint64(len(old.Value))
	}
	m.byteSize += uint64(len(key)) + uint64(len(value))
}

func (m *memtable) Delete(key Key) {
	old, replaced := m.tree.ReplaceOrInsert(KeyValue{Key: key, Value: nil})
	if replaced {
		m.byteSize -= uint64(len(old.Key)) + uint64(len(old.Value))
	}
	m.byteSize += uint64(len(key))
}

func (m *memtable) Len() int {
	return m.tree.Len()
}

func (m *memtable) Get(key Key) ([]byte, error) {
	entry, found := m.tree.Get(KeyValue{Key: key})

	if !found {
		return nil, ErrNotFound
	} else if entry.Value == nil {
		return nil, ErrDeleted
	}

	return entry.Value, nil
}

type memtableIterator struct {
	entries []KeyValue
	pos     int
}

func (it *memtableIterator) Next() bool {
	it.pos++
	return it.pos < len(it.entries)
}

func (it *memtableIterator) Current() KeyValue {
	return it.entries[it.pos]
}

func (it *memtableIterator) Close() error { return nil }

func (m *memtable) Scan(lo Key, hi Key) *memtableIterator {
	var entries []KeyValue
	m.tree.AscendRange(KeyValue{Key: lo}, KeyValue{Key: hi}, func(e KeyValue) bool {
		entries = append(entries, e)
		return true
	})
	return &memtableIterator{entries: entries, pos: -1}
}

func (m *memtable) ByteSize() uint64 {
	return m.byteSize
}

func (m *memtable) Entries() *memtableIterator {
	var entries []KeyValue
	m.tree.Ascend(func(e KeyValue) bool {
		entries = append(entries, e)
		return true
	})
	return &memtableIterator{entries: entries, pos: -1}
}
