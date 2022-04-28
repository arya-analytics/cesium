package persist

import (
	"cesium/kfs"
	"cesium/shut"
	"context"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
	"sync"
)

// Persist wraps a kfs.KFS and provides a mechanism for easily executing operations on it.
// Persist uses a pool of goroutines to execute operations concurrently.
// To create a new Persist, use persist.New.
type Persist[T comparable] struct {
	wg       *sync.WaitGroup
	sem      *semaphore.Weighted
	kfs      kfs.FS[T]
	shutdown shut.Shutdown
}

type Operation[T comparable] interface {
	// Context returns a context, that when canceled, represents a forced abort of the operation.
	Context() context.Context
	// FileKey returns the key of the file to which the operation applies.
	FileKey() T
	// SendError sends an error to the operation. This is only used for IO errors.
	SendError(error)
	// Exec is called by Persist to execute the operation. The provided file will have the key returned by FileKey.
	// The operation has a lock on the file during this time, and is free to make any modifications.
	Exec(f kfs.File)
}

// New creates a new Persist that wraps the provided kfs.KFS.
// maxProcs represents the maximum number of goroutines that will be used to execute operations concurrently.
// This value must be at least 1.
// shutdown is a shutdown.Shutdown that signals Persist to stop executing operations and exit gracefully.
func New[T comparable](kfs kfs.FS[T], maxProcs int64, shutdown shut.Shutdown) *Persist[T] {
	p := &Persist[T]{
		sem:      semaphore.NewWeighted(maxProcs),
		kfs:      kfs,
		wg:       &sync.WaitGroup{},
		shutdown: shutdown,
	}
	p.listenForShutdown()
	return p
}

// Exec queues a set of operations for execution. Operations are NOT guaranteed to execute in the order they are queued.
func (p *Persist[T]) Exec(ops []Operation[T]) {
	for _, op := range ops {
		if err := p.sem.Acquire(op.Context(), 1); err != nil {
			log.Warn(err)
		}
		p.wg.Add(1)
		go func(op Operation[T]) {
			defer func() {
				p.wg.Done()
			}()
			defer p.sem.Release(1)
			f, err := p.kfs.Acquire(op.FileKey())
			if err != nil {
				op.SendError(err)
				return
			}
			defer p.kfs.Release(op.FileKey())
			op.Exec(f)
		}(op)
	}
}

func (p *Persist[T]) listenForShutdown() {
	p.shutdown.Go(func(sig chan shut.Signal) error {
		<-sig
		p.wg.Wait()
		return nil
	})
}
