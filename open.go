package cesium

import (
	"github.com/arya-analytics/cesium/internal/core"
	"github.com/arya-analytics/x/kfs"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/pebblekv"
	"github.com/arya-analytics/x/signal"
	"github.com/cockroachdb/pebble"
	"path/filepath"
)

const channelCounterKey = "cs-nc"

// Open opens a new DB whose files are stored in the given directory.
// DB can be opened with a variety of options:
//
//	// Open a DB in memory.
//  cesium.MemBacked()
//
//  // Open a DB with the provided logger.
//	cesium.WithLogger(zap.NewNop())
//
//	// Bind an alamos.Experiment to register DB metrics.
//	cesium.WithExperiment(alamos.WithCancel("myExperiment"))
//
// 	// Override the default shutdown threshold.
//  cesium.WithShutdownThreshold(time.Second)
//
//  // Set custom shutdown options.
//	cesium.WithShutdownOptions()
//
// See each options documentation for more.
func Open(dirname string, opts ...Option) (DB, error) {
	ctx, shutdown := signal.Background()

	o := newOptions(dirname, opts...)

	// |||||| FILE SYSTEM ||||||

	fs, err := openFS(ctx, o)
	if err != nil {
		shutdown()
		return nil, err
	}

	// |||||| KV ||||||

	kve, err := openKV(o)
	if err != nil {
		shutdown()
		return nil, err
	}

	// |||||| CREATE ||||||

	create, err := startCreate(ctx, createConfig{
		exp:    o.exp,
		logger: o.logger,
		fs:     fs,
		kv:     kve,
	})
	if err != nil {
		shutdown()
		return nil, err
	}

	// |||||| RETRIEVE ||||||

	retrieve, err := startRetrieve(ctx, retrieveConfig{
		exp:    o.exp,
		logger: o.logger,
		fs:     fs,
		kv:     kve,
	})
	if err != nil {
		shutdown()
		return nil, err
	}

	// |||||| CHANNEL ||||||

	// a kv persisted counter that tracks the number of channels that a DB has created.
	// this is used to autogenerate unique keys for a channel.
	channelKeyCounter, err := kv.NewPersistedCounter(kve, []byte(channelCounterKey))
	if err != nil {
		shutdown()
		return nil, err
	}

	return &db{
		kv:                kve,
		shutdown:          shutdown,
		create:            create,
		retrieve:          retrieve,
		channelKeyCounter: channelKeyCounter,
		wg:                ctx,
	}, nil
}

func openFS(ctx signal.Context, opts *options) (core.FS, error) {
	fs, err := kfs.New[core.FileKey](
		filepath.Join(opts.dirname, cesiumDirectory),
		opts.kfs.opts...,
	)
	sync := &kfs.Sync[core.FileKey]{
		FS:       fs,
		Interval: opts.kfs.sync.interval,
		MaxAge:   opts.kfs.sync.maxAge,
	}
	sync.Start(ctx)
	return fs, err
}

func openKV(opts *options) (kv.KV, error) {
	pebbleDB, err := pebble.Open(filepath.Join(opts.dirname, kvDirectory), &pebble.Options{FS: opts.kvFS})
	return pebblekv.Wrap(pebbleDB), err
}
