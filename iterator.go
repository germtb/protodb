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
	Current() KeyValue
	Close() error
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

func (it *sliceIterator) Current() KeyValue { return it.entries[it.index] }
func (it *sliceIterator) Close() error      { return nil }
