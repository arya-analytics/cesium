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
	sd := shut.New(_opts.shutdownOpts...)
	fs := startFS(_opts, sd)
	kve, err := startKV(_opts)
	if err != nil {
		return nil, err
	}
	create, err := startCreatePipeline(fs, kve, _opts, sd)
	if err != nil {
		return nil, err
	}
	retrieve, err := startRetrievePipeline(fs, kve, _opts, sd)
	if err != nil {
		return nil, err
	}
	createChannel, retrieveChannel, err := startChannelPipeline(kve)
	if err != nil {
		return nil, err
	}
	return &db{
		kv:              kve,
		shutdown:        shut.NewGroup(sd),
		create:          create,
		retrieve:        retrieve,
		createChannel:   createChannel,
		retrieveChannel: retrieveChannel,
	}, nil
}

func startFS(opts *options, sd shut.Shutdown) fileSystem {
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

func startKV(opts *options) (kv.KV, error) {
	pebbleDB, err := pebble.Open(filepath.Join(opts.dirname, "pebble"), &pebble.Options{FS: opts.kvFS})
	return pebblekv.Wrap(pebbleDB), err
}

func startChannelPipeline(kve kv.KV) (query.Factory[CreateChannel], query.Factory[RetrieveChannel], error) {
	counter, err := kv.NewPersistedCounter(kve, []byte("cesium-nextChannel"))
	ckv := channelKV{kv: kve}
	cf := &createChannelFactory{exec: &createChannelQueryExecutor{ckv: ckv, counter: counter}}
	rf := &retrieveChannelFactory{exec: &retrieveChannelQueryExecutor{ckv: ckv}}
	return cf, rf, err
}
