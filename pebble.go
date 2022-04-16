package caesium

import (
	"github.com/cockroachdb/pebble"
)

// pebbleKV implements Engine and wraps a Pebble DB instance.
type pebbleKV struct {
	DB *pebble.DB
}

// Get implements the Engine interface.
func (pe pebbleKV) Get(key []byte) ([]byte, error) {
	v, c, err := pe.DB.Get(key)
	if err != nil {
		return v, err
	}
	return v, c.Close()
}

// Set implements the Engine interface.
func (pe pebbleKV) Set(key []byte, value []byte) error {
	return pe.DB.Set(key, value, pebble.NoSync)
}

// Delete implements the Engine interface.
func (pe pebbleKV) Delete(key []byte) error {
	return pe.DB.Delete(key, pebble.NoSync)
}

func (pe pebbleKV) Close() error {
	return pe.DB.Close()
}
