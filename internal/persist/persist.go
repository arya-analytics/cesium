package persist

import (
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/kfs"
	"github.com/arya-analytics/cesium/shut"
	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"
)

// Persist wraps a kfs.KFS and provides a mechanism for easily executing operations on it.
// Persist uses a pool of goroutines to execute operations concurrently.
// To create a new Persist, use persist.New.
type Persist[F comparable, O operation.Operation[F]] struct {
	sem *semaphore.Weighted
	kfs kfs.FS[F]
	ops chan O
	Config
}

const (
	// DefaultNumWorkers is the default maximum number of goroutines Persist can operate at once.
	DefaultNumWorkers = 50
)

type Config struct {
	// NumWorkers represents the number of goroutines that will be used to execute operations.
	// This value must be at least 1.
	NumWorkers int
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
		NumWorkers: DefaultNumWorkers,
	}
}

// New creates a new Persist that wraps the provided kfs.FS.
func New[F comparable, O operation.Operation[F]](kfs kfs.FS[F], config Config) *Persist[F, O] {
	p := &Persist[F, O]{kfs: kfs, Config: config, ops: make(chan O, config.NumWorkers)}
	p.start()
	return p
}

// Pipe queues a set of operations for execution. Operations are NOT guaranteed to execute in the order they are queued.
func (p *Persist[K, O]) Pipe(ops <-chan []O) {
	p.Shutdown.Go(func(sig chan shut.Signal) error {
		defer close(p.ops)
		for _ops := range ops {
			for _, op := range _ops {
				p.ops <- op
			}
		}
		return nil
	})
}

func (p *Persist[K, O]) start() {
	for i := 0; i < p.NumWorkers; i++ {
		p.Shutdown.Go(func(sig chan shut.Signal) error {
			for op := range p.ops {
				f, err := p.kfs.Acquire(op.FileKey())
				if err != nil {
					op.SendError(err)
					continue
				}
				op.Exec(f)
				p.kfs.Release(op.FileKey())
			}
			return nil
		})
	}
}
