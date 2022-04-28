package batch

import (
	"cesium/internal/persist"
	"cesium/shut"
)

type Operation[K comparable] interface {
	persist.Operation[K]
}

type Set[K comparable] struct {
	batches  map[chan []Operation[K]]Batch[K, Operation[K]]
	shutdown shut.Shutdown
}

func (s *Set[K]) Start() <-chan []Operation[K] {
	ch := make(chan []Operation[K])
	for pipe, batch := range s.batches {
		pipe, batch := pipe, batch
		s.shutdown.Go(func(sig chan shut.Signal) error {
			for {
				select {
				case <-sig:
					close(ch)
					return nil
				case ops := <-pipe:
					ch <- batch.Exec(ops)
				}
			}
		})
	}
	return ch
}

type Batch[K comparable, T Operation[K]] interface {
	Exec([]T) []T
}
