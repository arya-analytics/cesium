package cesium

import (
	"cesium/alamos"
	"cesium/internal/allocate"
	"cesium/internal/batch"
	"cesium/internal/kv"
	"cesium/internal/kv/pebblekv"
	"cesium/internal/lock"
	"cesium/internal/persist"
	"cesium/internal/queue"
	"cesium/kfs"
	"cesium/shut"
	"context"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"go.uber.org/zap"
	"path/filepath"
	"time"
)

// |||| DB ||||

type DB interface {

	// NewCreate opens a new Create query that is used for writing data to the DB. A simple synchronous create query
	// looks like the following:
	//
	//      // open the DB
	//  	ctx := Context.Background()
	//		db := cesium.Open("", cesium.MemBacked())
	//
	//      // create a new channel
	//      ch, err := cesium.NewCreateChannel().WithType(cesium.Float64).WithRate(5 * cesium.Hz).Exec(ctx)
	//		if err != nil {
	// 	    	 log.Fatal(err)
	//		}
	//
	//	     // create a new Segment to write. If you don't know what segmentKV are, check out the Segment documentation.
	//       segments := cesium.Segment{
	//    		ChannelKey: ch.Pk,
	//          Start: cesium.Now(),
	//          Data: cesium.MarshalFloat64([]{1.0, 2.0, 3.0})
	//		}
	//
	//		// open the query
	//		// db.sync is a helper that turns a typically async write into an acknowledged, sync write.
	//	    err := db.sync(ctx, db.NewCreate().WhereChannels(ch.Pk), []Segment{segments})
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//
	// The above example will create a new channel with the type Float64 and a data rate of 5 Hz. It will then write a
	// segment with 3 samples to the database.
	//
	// The Create query acquires write lock on the channels specified in WhereChannels. No other goroutine can write
	// to the channel until the Create query is closed.
	//
	// Asynchronous Create queries are the default in cesium to allow for network optimization and multi-segment write locks.
	// However, they are a bit more complex to write. See the following example:
	//
	//		// Assuming DB is opened, Channel is created, and a Segment is defined. See above example for details.
	//		// Start the create query. See Create.Stream for details on what each return value does.
	//		req, res, err := db.NewCreate().WhereChannels(ch.Pk).Stream(ctx)
	//
	//		// Start listening for errors. An EOF error means the Create query completed successfully (i.e. acknowledged all writes).
	//		wg := sync.WaitGroup{}
	//		go func() {
	// 			defer wg.Done()
	//			for resV := range res{
	//				if resV.Err == io.EOF {
	//					return
	//				}
	//				log.Fatal(resV.Err)
	//			}
	//		}
	//
	//		// Write the segment to the Create Request Stream.
	//      req <- segments
	//		// Close the Request Stream.
	//		close(req)
	//		// Wait for the Create query to acknowledge all writes.
	//		wg.Wait()
	//
	//		// Do what you want, but remember to close the database when done.
	//
	//  Adding and waiting for a sync.WaitGroup is a common pattern for Create queries, but is not required.
	//  cesium will ensure all writes are acknowledged upon DB.Close.
	NewCreate() Create

	// NewRetrieve opens a new Retrieve query that is used for retrieving data from the DB. A simple synchronous retrieve query
	// looks like the following:
	//
	// 	 	// open the DB create a channel, and write some data to it. See NewCreate for details.
	//
	//		// open the Retrieve query, and write the results into resSeg.
	//		// DB.Sync is a helper that turns a typically async read into sync read.
	//		// If you don't know what a Segment is, check out the Segment documentation.
	//		var resSeg []Segment
	//		err := db.sync(ctx, db.NewRetrieve().WhereTimeRange(cesium.TimeRangeMax).WhereChannels(ch.Pk), &resSeg)
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//
	// The above example retrieves all data from the channel and binds it into resSeg. It'stream possible to retrieve a subset
	// of data by time range by using the WhereTimeRange method and the supplied time range.
	//
	// Notes on Segmentation of Data:
	//		The Retrieve results will be returned as a set of segmentKV (Segment). The Segments are not guaranteed to be
	//		in chronological order. This is a performance optimization to allow for more efficient data retrieval.
	//
	//	 	The Segments are not guaranteed to be contiguous. Because of the unique Create pattern cesium uses, it'stream possible
	// 		to leave gaps in between segmentKV (these represent times when that particular sensor was inactive).
	//
	//      Retrieve also DOES NOT return partial Segments i.e if a query asks for the time range 0 to 10, and Segment A
	//		contains the data from time 0 to 6, and Segment B contains the data from 6 to 12, ALL Segment A will be returned
	// 		and ALL Segment B will be returned. Changes are in progress to allow for partial Segment returns.
	//
	// If no Segments are found, the Retrieve query will return an ErrNotFound error
	//
	// Asynchronous Retrieve queries are the default in cesium to allow for network optimization (i.e. send the data across
	// the network as you read more data from IO). However, they are a bit more complex to write. See the following example:
	//
	// 		// Assuming DB is opened and two Segment have been created for a channel with LKey channelKey. See NewCreate for details.
	//      // Start the retrieve query. See Retrieve.Stream for details on what each return value does.
	// 		// To cancel a query before it completes, cancel the Context provided to Retrieve.Stream.
	// 		ctx, cancel := Context.WithCancel(Context.Background())
	//		res, err := db.NewRetrieve().WhereTimeRange(cesium.TimeRangeMax).WhereChannels(channelKey).Stream(ctx)
	//
	//      var resSeg []Segment
	// 		for _, resV := range res {
	//			if resV.Err == io.EOF {
	//				break
	//			} else if resV.Err != nil {
	//				log.Fatal(resV.Err)
	//			}
	//			resSeg = append(resSeg, resV.Segments...)
	//		}
	//
	//      // Do what you want with the data, just remember to close the database when done.
	//
	NewRetrieve() Retrieve
	NewCreateChannel() CreateChannel
	NewRetrieveChannel() RetrieveChannel
	Sync(ctx context.Context, query Query, seg *[]Segment) error
	Close() error
}

func Open(dirname string, opts ...Option) (DB, error) {
	_opts := newOptions(dirname, opts...)
	sd := shut.New(_opts.shutdownOpts...)
	fs := startFS(_opts, sd)
	kve, err := startKV(_opts)
	if err != nil {
		return nil, err
	}
	createQExec, err := startCreatePipeline(fs, kve, _opts, sd)
	if err != nil {
		return nil, err
	}
	retrieveQExec := startRetrievePipeline(fs, kve, _opts, sd)
	createChannelQExec, retrieveChannelQExec, err := startChannelPipeline(kve)
	if err != nil {
		return nil, err
	}
	return &db{
		kv:              kve,
		shutdown:        shut.NewGroup(sd),
		create:          createQExec,
		retrieve:        retrieveQExec,
		createChannel:   createChannelQExec,
		retrieveChannel: retrieveChannelQExec,
	}, nil
}

type db struct {
	kv              kv.KV
	shutdown        *shut.Group
	create          queryExecutor
	retrieve        queryExecutor
	createChannel   queryExecutor
	retrieveChannel queryExecutor
}

func (d *db) NewCreate() Create {
	return newCreate(d.create)
}

func (d *db) NewRetrieve() Retrieve {
	return newRetrieve(d.retrieve)
}

func (d *db) NewCreateChannel() CreateChannel {
	return newCreateChannel(d.createChannel)
}

func (d *db) NewRetrieveChannel() RetrieveChannel {
	return newRetrieveChannel(d.retrieveChannel)
}

func (d *db) Sync(ctx context.Context, query Query, seg *[]Segment) error {
	switch q := query.Variant().(type) {
	case Create:
		return createSync(ctx, q, seg)
	case Retrieve:
		return retrieveSync(ctx, q, seg)
	}
	panic("only create and retrieve queries can be run synchronously")
}

func (d *db) Close() error {
	if err := d.shutdown.Sequential(); err != nil {
		return err
	}
	return d.kv.Close()
}

// |||| OPTIONS ||||

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
			o.exp = alamos.Sub(exp, fmt.Sprintf("cesium-%stream", o.dirname))
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

func WithShutdownOpts(opts ...shut.Option) Option {
	return func(o *options) {
		o.shutdownOpts = append(o.shutdownOpts, opts...)
	}
}

// |||||| START UP ||||||

func startCreatePipeline(fs fileSystem, kve kv.KV, opts *options, sd shut.Shutdown) (queryExecutor, error) {
	pst := persist.New[fileKey, batch.Operation[fileKey]](fs, 50, sd, opts.logger)
	q := &queue.Debounce[createOperation]{
		In:        make(chan []createOperation),
		Out:       make(chan []createOperation),
		Shutdown:  sd,
		Interval:  100 * time.Millisecond,
		Threshold: 50,
		Logger:    opts.logger,
	}
	q.Start()
	pst.Pipe(batch.Pipe[fileKey, createOperation](
		q.Out,
		sd,
		&batch.Create[fileKey, ChannelKey, createOperation]{}),
	)

	counter, err := kv.NewPersistedCounter(kve, []byte("cesium-nextFile"))
	if err != nil {
		return nil, err
	}
	nextFile := &fileCounter{PersistedCounter: *counter}

	alloc := allocate.New[ChannelKey, fileKey, Segment](nextFile, allocate.Config{
		MaxDescriptors: 10,
		MaxSize:        1e9,
	})

	return &createQueryExecutor{
		allocator: alloc,
		skv:       segmentKV{kv: kve},
		ckv:       channelKV{kv: kve},
		queue:     q.In,
		lock:      lock.NewMap[ChannelKey](),
		shutdown:  sd,
		logger:    opts.logger,
	}, nil
}

func startRetrievePipeline(fs fileSystem, kve kv.KV, opts *options, sd shut.Shutdown) queryExecutor {
	pst := persist.New[fileKey, batch.Operation[fileKey]](fs, 50, sd, opts.logger)
	q := &queue.Debounce[retrieveOperation]{
		In:        make(chan []retrieveOperation),
		Out:       make(chan []retrieveOperation),
		Shutdown:  sd,
		Interval:  100 * time.Millisecond,
		Threshold: 50,
		Logger:    opts.logger,
	}
	q.Start()
	pst.Pipe(batch.Pipe[fileKey, retrieveOperation](
		q.Out,
		sd,
		&batch.Retrieve[fileKey, retrieveOperation]{}),
	)
	return &retrieveQueryExecutor{
		parser:   retrieveParser{skv: segmentKV{kv: kve}, ckv: channelKV{kv: kve}, logger: opts.logger},
		queue:    q.In,
		shutdown: sd,
		logger:   opts.logger,
	}
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

func startChannelPipeline(kve kv.KV) (queryExecutor, queryExecutor, error) {
	counter, err := kv.NewPersistedCounter(kve, []byte("cesium-nextChannel"))
	ckv := channelKV{kv: kve}
	return &createChannelQueryExecutor{ckv: ckv, counter: counter},
		&retrieveChannelQueryExecutor{ckv: ckv}, err
}
