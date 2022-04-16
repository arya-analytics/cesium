package kv

import (
	"bytes"
	"caesium/pk"
	"io"
)

type Value []byte

type Key []byte

type Engine interface {
	Set(key Key, value Value) error
	Get(key Key) (Value, error)
	Delete(key Key) error
	Close() error
}

type FlushFill[T any] interface {
	Flush(io.Writer) error
	Fill(io.Reader) (T, error)
}

func SetWithPrefixedPK[T FlushFill[T]](e Engine, prefix Prefix, pk pk.PK, t T) error {
	b := new(bytes.Buffer)
	if err := t.Flush(b); err != nil {
		return err
	}
	return e.Set(PrefixedKey(prefix, pk.Bytes()), b.Bytes())
}

func GetWithPrefixedPK[T FlushFill[T]](e Engine, prefix Prefix, pk pk.PK, t T) (T, error) {
	b, err := e.Get(PrefixedKey(prefix, pk.Bytes()))
	if err != nil {
		return t, err
	}
	if t, err = t.Fill(bytes.NewBuffer(b)); err != nil {
		return t, err
	}
	return t, nil
}
