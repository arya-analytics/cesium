package persist

import (
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kfs"
	"github.com/arya-analytics/x/shutdown"
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
	confluence.UnarySink[[]O]
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
	Shutdown shutdown.Shutdown
}

func DefaultConfig() Config { return Config{NumWorkers: DefaultNumWorkers} }

// New creates a new Persist that wraps the provided kfs.FS.
func New[F comparable, O operation.Operation[F]](kfs kfs.FS[F], config Config) *Persist[F, O] {
	p := &Persist[F, O]{kfs: kfs, Config: config, ops: make(chan O, config.NumWorkers)}
	return p
}

func (p *Persist[K, O]) Flow(ctx confluence.Context) {
	p.start(ctx)
	ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
		for {
			select {
			case <-sig:
				return nil
			case ops := <-p.In.Outlet():
				for _, op := range ops {
					p.ops <- op
				}
			}
		}
	})
}

func (p *Persist[K, O]) start(ctx confluence.Context) {
	for i := 0; i < p.NumWorkers; i++ {
		ctx.Shutdown.Go(func(sig chan shutdown.Signal) error {
			for op := range p.ops {
				f, err := p.kfs.Acquire(op.FileKey())
				if err != nil {
					op.WriteError(err)
					continue
				}
				op.Exec(f)
				p.kfs.Release(op.FileKey())
			}
			return nil
		})
	}
}
