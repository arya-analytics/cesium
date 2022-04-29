package persist

import (
	"cesium/internal/operation"
	"cesium/kfs"
	"cesium/shut"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
	"sync"
)

// Persist wraps a kfs.KFS and provides a mechanism for easily executing operations on it.
// Persist uses a pool of goroutines to execute operations concurrently.
// To create a new Persist, use persist.New.
type Persist[K comparable, T operation.Operation[K]] struct {
	wg       *sync.WaitGroup
	sem      *semaphore.Weighted
	kfs      kfs.FS[K]
	shutdown shut.Shutdown
}

// New creates a new Persist that wraps the provided kfs.KFS.
// maxProcs represents the maximum number of goroutines that will be used to execute operations concurrently.
// This value must be at least 1.
// shutdown is a shutdown.Shutdown that signals Persist to stop executing operations and exit gracefully.
func New[K comparable, T operation.Operation[K]](kfs kfs.FS[K], maxProcs int64, shutdown shut.Shutdown) *Persist[K, T] {
	p := &Persist[K, T]{
		sem:      semaphore.NewWeighted(maxProcs),
		kfs:      kfs,
		wg:       &sync.WaitGroup{},
		shutdown: shutdown,
	}
	p.listenForShutdown()
	return p
}

func (p *Persist[K, T]) Pipe(opC chan []T) {
	p.shutdown.Go(func(sig chan shut.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case ops := <-opC:
				p.Exec(ops)
			}
		}
	})
}

// Exec queues a set of operations for execution. Operations are NOT guaranteed to execute in the order they are queued.
func (p *Persist[K, T]) Exec(ops []T) {
	for _, op := range ops {
		if err := p.sem.Acquire(op.Context(), 1); err != nil {
			log.Warn(err)
		}
		p.wg.Add(1)
		go func(op T) {
			defer p.wg.Done()
			defer p.sem.Release(1)
			f, err := p.kfs.Acquire(op.FileKey())
			if err != nil {
				op.SendError(err)
				return
			}
			op.Exec(f)
			p.kfs.Release(op.FileKey())
		}(op)
	}
}

func (p *Persist[K, T]) listenForShutdown() {
	p.shutdown.Go(func(sig chan shut.Signal) error {
		<-sig
		p.wg.Wait()
		return nil
	})
}
