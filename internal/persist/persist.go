package persist

import (
	"cesium/kfs"
	"cesium/shut"
	"context"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
	"sync"
)

type Persist[T comparable] struct {
	wg       *sync.WaitGroup
	sem      *semaphore.Weighted
	kfs      kfs.FS[T]
	shutdown shut.Shutdown
}

type Operation[T comparable] interface {
	Context() context.Context
	FileKey() T
	SendError(error)
	Exec(f kfs.File)
}

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

func (p *Persist[T]) listenForShutdown() {
	p.shutdown.Go(func(sig chan shut.Signal) error {
		<-sig
		p.wg.Wait()
		return nil
	})
}

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
