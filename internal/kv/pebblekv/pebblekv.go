package pebblekv

import (
	kvc "github.com/arya-analytics/cesium/internal/kv"
	"github.com/cockroachdb/pebble"
)

type pebbleKV struct {
	DB *pebble.DB
}

// Wrap wraps a pebble.DB to satisfy the kv.KV interface.
func Wrap(db *pebble.DB) kvc.KV {
	return &pebbleKV{DB: db}
}

func (kv pebbleKV) Get(key []byte) ([]byte, error) {
	v, c, err := kv.DB.Get(key)
	if err != nil {
		return v, err
	}
	return v, c.Close()
}

func (kv pebbleKV) Set(key []byte, value []byte) error {
	return kv.DB.Set(key, value, pebble.NoSync)
}

func (kv pebbleKV) Delete(key []byte) error {
	return kv.DB.Delete(key, pebble.NoSync)
}

func (kv pebbleKV) Close() error {
	return kv.DB.Close()
}

func (kv pebbleKV) IterPrefix(prefix []byte) kvc.Iterator {
	upper := func(b []byte) []byte {
		end := make([]byte, len(b))
		copy(end, b)
		for i := len(end) - 1; i >= 0; i-- {
			end[i] = end[i] + 1
			if end[i] != 0 {
				return end[:i+1]
			}
		}
		return nil
	}
	opts := func(prefix []byte) *pebble.IterOptions {
		return &pebble.IterOptions{LowerBound: prefix, UpperBound: upper(prefix)}
	}
	return kv.DB.NewIter(opts(prefix))
}

func (kv pebbleKV) IterRange(start, end []byte) kvc.Iterator {
	return kv.DB.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
}
