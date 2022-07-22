package cesium

import (
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/kfs"
	"github.com/arya-analytics/x/kv"
	"github.com/cockroachdb/pebble/vfs"
	"go.uber.org/zap"
	"time"
)

type Option func(*options)

type options struct {
	dirname string
	fs      struct {

		// kfs.BaseFS will be embedded as an option here.
		opts []kfs.Option
		sync struct {
			interval time.Duration
			maxAge   time.Duration
		}
	}
	exp    alamos.Experiment
	logger *zap.Logger
	kv     struct {
		external bool
		engine   kv.DB
		// fs is the file system we use for key-value storage. We don't use pebble's
		// vfs for the time series engine because it doesn't implement seek handles
		// for its files.
		fs vfs.FS
	}
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
	if o.fs.sync.interval == 0 {
		o.fs.sync.interval = 1 * time.Second
	}
	if o.fs.sync.maxAge == 0 {
		o.fs.sync.maxAge = 1 * time.Hour
	}

	// || LOGGER ||

	if o.logger == nil {
		o.logger = zap.NewNop()
	}
	o.fs.opts = append(o.fs.opts, kfs.WithLogger(o.logger))
	o.fs.opts = append(o.fs.opts, kfs.WithExperiment(o.exp))
	o.fs.opts = append(o.fs.opts, kfs.WithExtensionConfig(".tof"))
}

func MemBacked() Option {
	return func(o *options) {
		o.dirname = ""
		WithFS(vfs.NewMem(), kfs.NewMem())(o)
	}
}

func WithFS(vfs vfs.FS, baseKFS kfs.BaseFS) Option {
	return func(o *options) {
		o.kv.fs = vfs
		o.fs.opts = append(o.fs.opts, kfs.WithFS(baseKFS))
	}
}

func WithKVEngine(kv kv.DB) Option {
	return func(o *options) {
		o.kv.external = true
		o.kv.engine = kv
	}
}

func WithLogger(logger *zap.Logger) Option {
	return func(o *options) {
		o.logger = logger
	}
}

func WithExperiment(exp alamos.Experiment) Option {
	return func(o *options) {
		o.exp = alamos.Sub(exp, "cesium")
	}
}
