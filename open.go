package cesium

import (
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/kv/pebblekv"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/cesium/kfs"
	"github.com/arya-analytics/cesium/shut"
	"github.com/cockroachdb/pebble"
	"path/filepath"
)

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
//	cesium.WithExperiment(alamos.New("myExperiment"))
//
// 	// Override the default shutdown threshold.
//  cesium.WithShutdownThreshold(time.Second)
//
//  // Set custom shutdown options.
//	cesium.WithShutdownOptions()
//
// See each options documentation for more.
func Open(dirname string, opts ...Option) (DB, error) {
	_opts := newOptions(dirname, opts...)

	// |||||| SHUTDOWN ||||||

	shutdown := shut.New(_opts.shutdownOpts...)

	// |||||| FILE SYSTEM ||||||

	fs := openFS(_opts, shutdown)

	// |||||| KV ||||||

	kve, err := openKV(_opts)
	if err != nil {
		return nil, err
	}

	// |||||| CREATE ||||||

	create, err := startCreate(createConfig{
		exp:      _opts.exp,
		logger:   _opts.logger,
		shutdown: shutdown,
		fs:       fs,
		kv:       kve,
	})
	if err != nil {
		return nil, err
	}

	// |||||| RETRIEVE ||||||

	retrieve, err := startRetrieve(retrieveConfig{
		exp:      _opts.exp,
		logger:   _opts.logger,
		shutdown: shutdown,
		fs:       fs,
		kv:       kve,
	})
	if err != nil {
		return nil, err
	}

	// |||||| CHANNEL ||||||

	createChannel, retrieveChannel, err := startChannel(kve)
	if err != nil {
		return nil, err
	}

	return &db{
		kv:              kve,
		shutdown:        shut.NewGroup(shutdown),
		create:          create,
		retrieve:        retrieve,
		createChannel:   createChannel,
		retrieveChannel: retrieveChannel,
	}, nil
}

func openFS(opts *options, sd shut.Shutdown) fileSystem {
	fs := kfs.New[fileKey](opts.dirname, opts.kfs.opts...)
	sync := &kfs.Sync[fileKey]{
		FS:       fs,
		Interval: opts.kfs.sync.interval,
		MaxAge:   opts.kfs.sync.maxAge,
		Shutter:  sd,
	}
	sync.Start()
	return fs
}

func openKV(opts *options) (kv.KV, error) {
	pebbleDB, err := pebble.Open(filepath.Join(opts.dirname, "pebble"), &pebble.Options{FS: opts.kvFS})
	return pebblekv.Wrap(pebbleDB), err
}

const channelCounterKey = "cs-nc"

func startChannel(kve kv.KV) (query.Factory[CreateChannel], query.Factory[RetrieveChannel], error) {
	// a kv persisted counter that tracks the number of channels that a DB has created.
	// this is used to autogenerate unique keys for a channel.
	cCount, err := kv.NewPersistedCounter(kve, []byte(channelCounterKey))
	if err != nil {
		return nil, nil, err
	}

	ckv := channelKV{kv: kve}

	cf := &createChannelFactory{exec: &createChannelQueryExecutor{ckv: ckv, counter: cCount}}

	rf := &retrieveChannelFactory{exec: &retrieveChannelQueryExecutor{ckv: ckv}}

	return cf, rf, err
}