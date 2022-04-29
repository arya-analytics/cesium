package lock

import (
	"cesium/internal/errutil"
	"errors"
	"sync"
)

type MapHook[K comparable, I MapItem[K]] interface {
	Exec(items ...I) error
}

type MapItem[K comparable] interface {
	Key() K
}

type Map[K comparable, I MapItem[K]] struct {
	mu    sync.Mutex
	locks map[K]bool
	hooks []MapHook[K, I]
}

func NewMap[K comparable, I MapItem[K]](hooks ...MapHook[K, I]) *Map[K, I] {
	return &Map[K, I]{locks: make(map[K]bool), hooks: hooks}
}

func (m *Map[K, I]) Acquire(items ...I) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range items {
		locked, ok := m.locks[item.Key()]
		if !ok || !locked {
			m.locks[item.Key()] = true
		}
		if locked {
			return errors.New("item already locked")
		}
	}
	return m.execHooks(items...)
}

func (m *Map[K, I]) execHooks(items ...I) error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, hook := range m.hooks {
		c.Exec(func() error { return hook.Exec(items...) })
	}
	return c.Error()
}

func (m *Map[K, I]) Release(items ...I) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, item := range items {
		m.locks[item.Key()] = false
	}
}
