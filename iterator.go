package protodb

type Key = []byte

type KeyValue struct {
	Key   Key
	Value []byte
}

type Iterator interface {
	// Next advances the iterator one step in whichever direction was baked
	// in at construction (forward for Scan, reverse for ReverseScan).
	// Returns false when exhausted.
	Next() bool
	// Current returns the current key/value. Only valid after Next() returns true.
	Current() KeyValue
	// Close releases underlying resources (e.g. file handles). Safe to call
	// after Next returns false.
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

type skipTombstonesIterator struct {
	inner Iterator
}

func skipTombstones(inner Iterator) Iterator {
	return &skipTombstonesIterator{inner: inner}
}

func (it *skipTombstonesIterator) Next() bool {
	for it.inner.Next() {
		if it.inner.Current().Value != nil {
			return true
		}
	}
	return false
}

func (it *skipTombstonesIterator) Current() KeyValue { return it.inner.Current() }
func (it *skipTombstonesIterator) Close() error      { return it.inner.Close() }
