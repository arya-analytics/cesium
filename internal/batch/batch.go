package batch

import (
	"cesium/internal/persist"
	"cesium/kfs"
	"cesium/shut"
	"context"
)

// |||||| OPERATION ||||||

type Operation[K comparable] interface {
	persist.Operation[K]
}

// |||||| BATCH |||||||

type Batch[K comparable, T Operation[K]] interface {
	Exec([]T) []Operation[K]
}

// |||||| PIPE ||||||

func Pipe[K comparable, T Operation[K]](req <-chan []T, sd shut.Shutdown, batch Batch[K, T]) <-chan []Operation[K] {
	res := make(chan []Operation[K])
	sd.Go(func(sig chan shut.Signal) error {
		for {
			select {
			case <-sig:
				close(res)
				return nil
			case ops := <-req:
				res <- batch.Exec(ops)
			}
		}
	})
	return res
}

// |||||| OPERATION SET ||||||

type OperationSet[K comparable, T Operation[K]] []T

func (o OperationSet[K, T]) Context() context.Context {
	return o[0].Context()
}

func (o OperationSet[K, T]) FileKey() K {
	return o[0].FileKey()
}

func (o OperationSet[K, T]) Exec(f kfs.File) {
	for _, op := range o {
		op.Exec(f)
	}
}

func (o OperationSet[K, T]) SendError(err error) {
	for _, op := range o {
		op.SendError(err)
	}
}
