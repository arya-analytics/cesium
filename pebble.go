package cesium

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

func (pe pebbleKV) IterPrefix(prefix []byte) kvIterator {
	keyUpperBound := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil // no upper-bound
	}

	prefixIterOptions := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{
			LowerBound: prefix,
			UpperBound: keyUpperBound(prefix),
		}
	}
	return pe.DB.NewIter(prefixIterOptions(prefix))
}

func (pe pebbleKV) IterRange(start, end []byte) kvIterator {
	return pe.DB.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
}
