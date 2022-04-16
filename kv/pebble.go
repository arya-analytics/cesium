package kv

import (
	"github.com/cockroachdb/pebble"
)

// PebbleEngine implements Engine and wraps a Pebble DB instance.
type PebbleEngine struct {
	DB *pebble.DB
}

// Get implements the Engine interface.
func (pe PebbleEngine) Get(key Key) (Value, error) {
	v, c, err := pe.DB.Get(key)
	if err != nil {
		return v, err
	}
	return v, c.Close()
}

// Set implements the Engine interface.
func (pe PebbleEngine) Set(key Key, value Value) error {
	return pe.DB.Set(key, value, pebble.NoSync)
}

// Delete implements the Engine interface.
func (pe PebbleEngine) Delete(key Key) error {
	return pe.DB.Delete(key, pebble.NoSync)
}

func (pe PebbleEngine) Close() error {
	return pe.DB.Close()
}
