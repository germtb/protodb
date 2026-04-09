package protodb

type Iterator interface {
	// Next advances the iterator. Returns false when exhausted.
	Next() bool
	// Key returns the current key. Only valid after Next() returns true.
	Key() uint64
	// Value returns the current value. Nil means tombstone.
	Value() []byte
}
