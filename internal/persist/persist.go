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
	wg  *sync.WaitGroup
	sem *semaphore.Weighted
	kfs kfs.FS[F]
	Config
}

const (
	// DefaultMaxRoutines is the default maximum number of goroutines Persist can operate at once.
	DefaultMaxRoutines = 50
)

type Config struct {
	// MaxRoutines represents the maximum number of goroutines that will be used to execute operations.
	// This value must be at least 1.
	MaxRoutines int64
	// Persist will log events to this Logger.
	Logger *zap.Logger
	// Shutdown will be used to gracefully stop Persist by waiting for all executed operations to complete.
	// NOTE: Exec will continue accepting operations after the Shutdown is called. It is up to the caller
	// to ensure that the flow of operations is halted beforehand. Shutdown is not required if the caller
	// is tracking the completion of operations internally.
	Shutdown shut.Shutdown
}

func DefaultConfig() Config {
	return Config{
		MaxRoutines: DefaultMaxRoutines,
	}
}

// New creates a new Persist that wraps the provided kfs.FS.
func New[F comparable, O operation.Operation[F]](kfs kfs.FS[F], config Config) *Persist[F, O] {
	p := &Persist[F, O]{
		sem:    semaphore.NewWeighted(config.MaxRoutines),
		kfs:    kfs,
		wg:     &sync.WaitGroup{},
		Config: config,
	}
	if p.Shutdown != nil {
		p.listenForShutdown()
	}
	return p
}

// Exec queues a set of operations for execution. Operations are NOT guaranteed to execute in the order they are queued.
func (p *Persist[K, O]) Exec(ops []O) {
	p.Logger.Debug("executing operations", zap.Int("count", len(ops)))
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
	p.Shutdown.Go(func(sig chan shut.Signal) error {
		<-sig
		p.Logger.Info("persist shutting down. waiting for operations to complete.")
		p.wg.Wait()
		p.Logger.Info("persist Shutdown complete.")
		return nil
	})
}
