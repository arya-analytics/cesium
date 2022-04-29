package cesium

import (
	"cesium/alamos"
	"cesium/internal/allocate"
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
	o := newOptions(dirname, opts...)
	sd := shut.New(o.shutdownOpts...)

	// |||||| KFS |||||||

	fs := kfs.New[fileKey](o.dirname, o.kfs.opts...)
	sync := &kfs.Sync[fileKey]{
		FS:       fs,
		Interval: o.kfs.sync.interval,
		MaxAge:   o.kfs.sync.maxAge,
		Shutter:  sd,
	}
	sync.Start()

	// |||||| KV |||||||

	pebbleDB, err := pebble.Open(o.dirname, &pebble.Options{FS: o.kvFS})
	if err != nil {
		return nil, err
	}
	kve := pebblekv.Wrap(pebbleDB)
	skv := segmentKV{kv: kve}
	ckv := channelKV{kv: kve}

	// |||||| PERSIST ||||||

	// || CREATE ||

	createPst := persist.New[fileKey, createOperation](fs, 50, sd)
	createOpRequests := make(chan []createOperation)
	createOpResponses := make(chan []createOperation)
	createQueue := &queue.Debounce[createOperation]{
		Requests:  createOpRequests,
		Responses: createOpResponses,
		Shutdown:  sd,
		Interval:  1 * time.Second,
		Threshold: 50,
	}
	createQueue.Start()
	createPst.Pipe(createOpResponses)

	// || RETRIEVE ||

	retrievePst := persist.New[fileKey, retrieveOperation](fs, 50, sd)
	retrieveOpRequests := make(chan []retrieveOperation)
	retrieveOpResponses := make(chan []retrieveOperation)
	retrieveQueue := &queue.Debounce[retrieveOperation]{
		Requests:  retrieveOpRequests,
		Responses: retrieveOpResponses,
		Shutdown:  sd,
		Interval:  1 * time.Second,
		Threshold: 50,
	}
	retrieveQueue.Start()
	retrievePst.Pipe(retrieveOpResponses)

	// ||||||| CREATE ||||||

	nextFileCounter, err := kv.NewPersistedCounter(kve, []byte("cesium-nextFile"))
	if err != nil {
		return nil, err
	}
	nextFile := &fileCounter{PersistedCounter: *nextFileCounter}
	segAlloc := allocate.New[ChannelKey, fileKey, Segment](nextFile, allocate.Config{
		MaxDescriptors: 50,
		MaxSize:        1e9,
	})
	createQExec := &createQueryExecutor{
		allocator: segAlloc,
		skv:       skv,
		ckv:       ckv,
		queue:     createOpRequests,
		lock:      lock.NewMap[ChannelKey](),
		shutdown:  sd,
	}

	// |||||| RETRIEVE ||||||

	retrieveQExec := &retrieveQueryExecutor{
		parser:   retrieveParser{skv: skv, ckv: ckv},
		queue:    retrieveOpRequests,
		shutdown: sd,
	}

	// |||||| CHANNEL |||||

	nextChannelCounter, err := kv.NewPersistedCounter(kve, []byte("cesium-nextChannel"))
	cChannelQExec := &createChannelQueryExecutor{ckv: ckv, counter: nextChannelCounter}
	rChannelQExec := &retrieveChannelQueryExecutor{ckv: ckv}

	return &db{
		kv:              kve,
		shutdown:        shut.NewGroup(sd),
		create:          createQExec,
		retrieve:        retrieveQExec,
		createChannel:   cChannelQExec,
		retrieveChannel: rChannelQExec,
	}, nil
}

type db struct {
	kv              kv.KV
	shutdown        *shut.Group
	create          *createQueryExecutor
	retrieve        *retrieveQueryExecutor
	createChannel   *createChannelQueryExecutor
	retrieveChannel *retrieveChannelQueryExecutor
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
}

func newOptions(dirname string, opts ...Option) *options {
	o := &options{dirname: dirname}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func MemBacked() Option {
	return func(o *options) {
		o.dirname = ""
		o.kfs.opts = append(o.kfs.opts, kfs.WithFS(kfs.NewMem()))
		o.kvFS = vfs.NewMem()
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
