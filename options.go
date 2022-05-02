package cesium

import (
	"github.com/arya-analytics/cesium/alamos"
	"github.com/arya-analytics/cesium/kfs"
	"github.com/arya-analytics/cesium/shut"
	"github.com/cockroachdb/pebble/vfs"
	"go.uber.org/zap"
	"time"
)

type Option func(*options)

type options struct {
	dirname string
	kfs     struct {
		opts []kfs.Option
		sync struct {
			interval time.Duration
			maxAge   time.Duration
		}
	}
	kvFS         vfs.FS
	exp          alamos.Experiment
	shutdownOpts []shut.Option
	logger       *zap.Logger
}

func newOptions(dirname string, opts ...Option) *options {
	o := &options{dirname: dirname}
	for _, opt := range opts {
		opt(o)
	}
	mergeDefaultOptions(o)
	return o
}

func mergeDefaultOptions(o *options) {
	if o.kfs.sync.interval == 0 {
		o.kfs.sync.interval = 1 * time.Second
	}
	if o.kfs.sync.maxAge == 0 {
		o.kfs.sync.maxAge = 1 * time.Hour
	}
	if o.shutdownOpts == nil {
		o.shutdownOpts = []shut.Option{}
	}

	// || LOGGER ||

	if o.logger == nil {
		o.logger = zap.NewNop()
	}
	o.kfs.opts = append(o.kfs.opts, kfs.WithLogger(o.logger))
	o.kfs.opts = append(o.kfs.opts, kfs.WithExperiment(o.exp))
	o.kfs.opts = append(o.kfs.opts, kfs.WithSuffix(".cseg"))
}

func MemBacked() Option {
	return func(o *options) {
		o.dirname = ""
		o.kfs.opts = append(o.kfs.opts, kfs.WithFS(kfs.NewMem()))
		o.kvFS = vfs.NewMem()
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithExperiment(exp alamos.Experiment) Option {
	return func(o *options) {
		if exp != nil {
			o.exp = alamos.Sub(exp, "cesium")
		} else {
			o.exp = exp
		}
	}
}

func WithShutdownThreshold(threshold time.Duration) Option {
	return func(o *options) {
		o.shutdownOpts = append(o.shutdownOpts, shut.WithThreshold(threshold))
	}
}

func WithShutdownOptions(opts ...shut.Option) Option {
	return func(o *options) {
		o.shutdownOpts = append(o.shutdownOpts, opts...)
	}
}
