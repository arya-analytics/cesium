package persist

import (
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/kfs"
	"github.com/arya-analytics/cesium/shut"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
	"sync"
)

// Persist wraps a kfs.KFS and provides a mechanism for easily executing operations on it.
// Persist uses a pool of goroutines to execute operations concurrently.
// To create a new Persist, use persist.New.
type Persist[F comparable, O operation.Operation[F]] struct {
	wg       *sync.WaitGroup
	sem      *semaphore.Weighted
	kfs      kfs.FS[F]
	shutdown shut.Shutdown
	logger   *zap.Logger
}

// New creates a new Persist that wraps the provided kfs.KFS.
// maxProcs represents the maximum number of goroutines that will be used to execute operations concurrently.
// This value must be at least 1.
// shutdown is a shutdown.Shutdown that signals Persist to stop executing operations and exit gracefully.
func New[F comparable, O operation.Operation[F]](kfs kfs.FS[F], maxProcs int64, shutdown shut.Shutdown, logger *zap.Logger) *Persist[F, O] {
	p := &Persist[F, O]{
		sem:      semaphore.NewWeighted(maxProcs),
		kfs:      kfs,
		wg:       &sync.WaitGroup{},
		shutdown: shutdown,
		logger:   logger,
	}
	p.listenForShutdown()
	return p
}

// Exec queues a set of operations for execution. Operations are NOT guaranteed to execute in the order they are queued.
func (p *Persist[K, O]) Exec(ops []O) {
	p.logger.Debug("executing operations", zap.Int("count", len(ops)))
	for _, op := range ops {
		if err := p.sem.Acquire(op.Context(), 1); err != nil {
			log.Warn(err)
		}
		p.wg.Add(1)
		go func(op O) {
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

func (p *Persist[K, O]) listenForShutdown() {
	p.shutdown.Go(func(sig chan shut.Signal) error {
		<-sig
		p.logger.Info("persist shutting down. waiting for operations to complete.")
		p.wg.Wait()
		p.logger.Info("persist shutdown complete.")
		return nil
	})
}
