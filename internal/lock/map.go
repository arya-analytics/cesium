package lock

import (
	"errors"
	"sync"
)

type Map[K comparable] struct {
	mu    *sync.Mutex
	locks map[K]bool
}

func NewMap[K comparable]() Map[K] {
	return Map[K]{locks: make(map[K]bool), mu: &sync.Mutex{}}
}

func (m Map[K]) Acquire(keys ...K) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		locked, ok := m.locks[key]
		if !ok || !locked {
			m.locks[key] = true
		}
		if locked {
			return errors.New("lock already held")
		}
	}
	return nil
}

func (m Map[K]) Release(keys ...K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, key := range keys {
		m.locks[key] = false
	}
}
