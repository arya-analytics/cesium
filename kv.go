package cesium

import (
	"bytes"
	"cesium/util/errutil"
	"io"
)

// |||||| ENGINE ||||||

type kvEngine interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	IterPrefix(prefix []byte) kvIterator
	IterRange(start []byte, end []byte) kvIterator
	Delete(key []byte) error
	Close() error
}

type kvIterator interface {
	First() bool
	Last() bool
	Next() bool
	Key() []byte
	Valid() bool
	Value() []byte
	Close() error
}

// |||||| PREFIX ||||||

func generateKey(elems ...interface{}) []byte {
	b := new(bytes.Buffer)
	cw := errutil.NewCatchWrite(b)
	for _, e := range elems {
		switch e.(type) {
		case string:
			cw.Write([]byte(e.(string)))
		default:
			cw.Write(e)
		}
	}
	if cw.Error() != nil {
		panic(cw.Error())
	}
	return b.Bytes()
}

// |||||| FLUSH ||||||

type flush[T any] interface {
	flush(writer io.Writer) error
	fill(reader io.Reader) (T, error)
}

type flushKV[T any] struct {
	kvEngine
}

func (kv flushKV[T]) flush(key []byte, f flush[T]) error {
	b := new(bytes.Buffer)
	if err := f.flush(b); err != nil {
		return err
	}
	return kv.Set(key, b.Bytes())
}

func (kv flushKV[T]) fill(key []byte, f flush[T]) (T, error) {
	b, err := kv.Get(key)
	nt, fErr := f.fill(bytes.NewReader(b))
	if err != nil {
		return nt, err
	}
	return nt, fErr
}
