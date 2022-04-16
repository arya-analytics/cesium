package caesium

import (
	"bytes"
	"io"
)

// |||||| ENGINE ||||||

type kvEngine interface {
	Set(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	Close() error
}

// |||||| PREFIX ||||||

type kvPrefix struct {
	prefix []byte
}

func (p kvPrefix) key(key []byte) []byte {
	return bytes.Join([][]byte{p.prefix, key}, nil)
}

func (p kvPrefix) pk(pk PK) []byte {
	return p.key(pk.Bytes())
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
