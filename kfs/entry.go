package kfs

import (
	"github.com/arya-analytics/cesium/internal/lock"
	"time"
)

type entry[T comparable] struct {
	BaseFile
	lock.Lock
	ls  time.Time
	key T
}

func (e *entry[T]) Age() time.Duration {
	return time.Since(e.ls)
}

func (e *entry[T]) Sync() error {
	e.ls = time.Now()
	return e.BaseFile.Sync()
}

func (e *entry[T]) Key() T {
	return e.key
}

func newEntry[T comparable](key T, f BaseFile) File[T] {
	return &entry[T]{
		Lock:     lock.New(),
		BaseFile: f,
		ls:       time.Now(),
		key:      key,
	}
}
