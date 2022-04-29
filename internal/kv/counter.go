package kv

import (
	"cesium/internal/binary"
	"errors"
	"github.com/cockroachdb/pebble"
	"io"
)

type PersistedCounter struct {
	kve   KV
	key   []byte
	value int
}

func NewPersistedCounter(kv KV, key []byte) (*PersistedCounter, error) {
	c := &PersistedCounter{kve: kv, key: key}
	err := Load(kv, c.key, c)
	if errors.Is(err, pebble.ErrNotFound) {
		c.value = 0
	}
	return c, err
}

func (c *PersistedCounter) ByteValue() []byte {
	bv, _ := binary.Marshal(c.value)
	return bv
}

func (c *PersistedCounter) Load(r io.Reader) error {
	return binary.Read(r, &c.value)
}

func (c *PersistedCounter) Flush(w io.Writer) error {
	return binary.Write(w, c.value)
}

func (c *PersistedCounter) flushShelf() error {
	return Flush(c.kve, c.key, c)
}

func (c *PersistedCounter) Increment(values ...int) (int, error) {
	if len(values) == 0 {
		c.value++
	}
	for _, v := range values {
		c.value += v
	}
	return c.value, c.flushShelf()
}

func (c *PersistedCounter) Decrement(values ...int) (int, error) {
	if len(values) == 0 {
		c.value--
	}
	for _, v := range values {
		c.value -= v
	}
	return c.value, c.flushShelf()
}
