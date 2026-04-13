package protodb

type Key = []byte

type KeyValue struct {
	Key   Key
	Value []byte
}

type Iterator interface {
	// Next advances the iterator. Returns false when exhausted.
	Next() bool
	// Key returns the current key. Only valid after Next() returns true.
	Key() Key
	// Value returns the current value. Nil means tombstone.
	Value() []byte
}

type sliceIterator struct {
	entries []KeyValue
	index   int
}

func iter(slice []KeyValue) *sliceIterator {
	return &sliceIterator{entries: slice, index: -1}
}

func (it *sliceIterator) Next() bool {
	it.index++
	return it.index < len(it.entries)
}

func (it *sliceIterator) Key() Key      { return it.entries[it.index].Key }
func (it *sliceIterator) Value() []byte { return it.entries[it.index].Value }
