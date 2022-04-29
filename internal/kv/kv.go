package kv

// |||||| ENGINE ||||||

type KV interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
	IteratorEngine
}

type IteratorEngine interface {
	IterPrefix(prefix []byte) Iterator
	IterRange(start []byte, end []byte) Iterator
}

type Iterator interface {
	First() bool
	Last() bool
	Next() bool
	Key() []byte
	Valid() bool
	Value() []byte
	Close() error
}
