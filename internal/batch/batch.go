package batch

import (
	"cesium/internal/persist"
)

type Operation[K comparable] interface {
	persist.Operation[K]
}

type Set[K comparable] struct {
	batches map[chan []Operation[K]]Batch[K, Operation[K]]
}

func (s *Set[K]) Start() <-chan []Operation[K] {
	ch := make(chan []Operation[K])
	for pipe, batch := range s.batches {
		go func(pipe chan []Operation[K], batch Batch[K, Operation[K]]) {
			for ops := range pipe {
				ch <- batch.Exec(ops)
			}
		}(pipe, batch)
	}
	return ch
}

type Batch[K comparable, T Operation[K]] interface {
	Exec([]T) []T
}
