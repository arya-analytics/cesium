package kv

import (
	"bytes"
	"io"
)

type Flusher interface {
	Flush(w io.Writer) error
}

func Flush(kv KV, key []byte, flusher Flusher) error {
	b := new(bytes.Buffer)
	if err := flusher.Flush(b); err != nil {
		return err
	}
	return kv.Set(key, b.Bytes())
}

type Loader interface {
	Load(r io.Reader) error
}

func Load(kv KV, key []byte, loader Loader) error {
	b, err := kv.Get(key)
	if err != nil {
		return err
	}
	return LoadBytes(b, loader)
}

func LoadBytes(b []byte, loader Loader) error {
	return loader.Load(bytes.NewReader(b))
}
