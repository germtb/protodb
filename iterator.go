package protodb

type Key = []byte

type Iterator interface {
	// Next advances the iterator. Returns false when exhausted.
	Next() bool
	// Key returns the current key. Only valid after Next() returns true.
	Key() Key
	// Value returns the current value. Nil means tombstone.
	Value() []byte
}
