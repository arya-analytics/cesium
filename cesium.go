package cesium

import (
	"cesium/alamos"
	"cesium/shut"
	"context"
	"fmt"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	log "github.com/sirupsen/logrus"
	"path/filepath"
	"time"
)

// |||| DB ||||

type DB interface {

	// NewCreate opens a new Create query that is used for writing data to the DB. A simple synchronous create query
	// looks like the following:
	//
	//      // open the DB
	//  	ctx := context.Background()
	//		db := cesium.Open("", cesium.MemBacked())
	//
	//      // create a new channel
	//      ch, err := cesium.NewCreateChannel().WithType(cesium.Float64).WithRate(5 * cesium.Hz).Exec(ctx)
	//		if err != nil {
	// 	    	 log.Fatal(err)
	//		}
	//
	//	     // create a new Segment to write. If you don't know what segments are, check out the Segment documentation.
	//       seg := cesium.Segment{
	//    		ChannelPK: ch.Pk,
	//          Start: cesium.Now(),
	//          Data: cesium.MarshalFloat64([]{1.0, 2.0, 3.0})
	//		}
	//
	//		// open the query
	//		// db.Sync is a helper that turns a typically async write into an acknowledged, sync write.
	//	    err := db.Sync(ctx, db.NewCreate().WhereChannels(ch.Pk), []Segment{seg})
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
	//      req <- seg
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
	//		err := db.Sync(ctx, db.NewRetrieve().WhereTimeRange(cesium.TimeRangeMax).WhereChannels(ch.Pk), &resSeg)
	//		if err != nil {
	//			log.Fatal(err)
	//		}
	//
	// The above example retrieves all data from the channel and binds it into resSeg. It's possible to retrieve a subset
	// of data by time range by using the WhereTimeRange method and the supplied time range.
	//
	// Notes on Segmentation of Data:
	//		The Retrieve results will be returned as a set of segments (Segment). The Segments are not guaranteed to be
	//		in chronological order. This is a performance optimization to allow for more efficient data retrieval.
	//
	//	 	The Segments are not guaranteed to be contiguous. Because of the unique Create pattern cesium uses, it's possible
	// 		to leave gaps in between segments (these represent times when that particular sensor was inactive).
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
	// 		// Assuming DB is opened and two Segment have been created for a channel with PK cpk. See NewCreate for details.
	//      // Start the retrieve query. See Retrieve.Stream for details on what each return value does.
	// 		// To cancel a query before it completes, cancel the context provided to Retrieve.Stream.
	// 		ctx, cancel := context.WithCancel(context.Background())
	//		res, err := db.NewRetrieve().WhereTimeRange(cesium.TimeRangeMax).WhereChannels(cpk).Stream(ctx)
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
	// NewDelete opens a new Delete query.
	NewDelete() Delete
	NewCreateChannel() CreateChannel
	NewRetrieveChannel() RetrieveChannel
	Sync(ctx context.Context, query Query, seg *[]Segment) error
	Close() error
}

func Open(dirname string, opts ...Option) (DB, error) {
	o := newOptions(dirname, opts...)

	shutter := shut.New(o.shutdownOpts...)

	// |||| PERSIST ||||

	kve, err := openPebbleKVDB(o)
	if err != nil {
		return nil, err
	}
	pst := openPersist(o)

	// |||| BATCH QUEUE ||||

	batchQueue := newQueue(batchRunner{
		persist: pst,
		batch:   batchSet{retrieveBatch{}, createBatchFileChannel{}},
	}.exec, shutter)
	go batchQueue.goTick()

	// |||| RUNNER ||||

	skv := newSegmentKV(kve)
	ckv := newChannelKV(kve)
	fa := newFileAllocate(skv, o.exp)

	crSvc := newCreateChannelRunService(ckv)
	rcSvc := newRetrieveChannelRunService(ckv)

	cr := newCreateRunService(fa, skv, ckv)
	rc := newRetrieveRunService(skv, ckv)

	runner := &run{
		batchQueue: batchQueue.ops,
		services:   []runService{crSvc, rcSvc, cr, rc},
	}

	return &db{
		dirname: dirname,
		opts:    o,
		kve:     kve,
		runner:  runner,
		shutter: shutter,
	}, nil
}

type db struct {
	dirname string
	opts    *options
	shutter shut.Shutter
	runner  *run
	kve     kvEngine
}

func (d *db) NewCreate() Create {
	return newCreate(d.runner)
}

func (d *db) NewRetrieve() Retrieve {
	return newRetrieve(d.runner)
}

func (d *db) NewDelete() Delete {
	return newDelete(d.runner)
}

func (d *db) NewCreateChannel() CreateChannel {
	return newCreateChannel(d.runner)
}

func (d *db) NewRetrieveChannel() RetrieveChannel {
	return newRetrieveChannel(d.runner)
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
	log.Info("[cesium.DB] shutting down gracefully")
	dbErr := d.shutter.Close()
	kvErr := d.kve.Close()
	if dbErr != nil {
		return dbErr
	}
	return kvErr
}

// |||| OPTIONS ||||

type Option func(*options)

type options struct {
	dirname      string
	memBacked    bool
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
		o.memBacked = true
	}
}

func WithExperiment(exp alamos.Experiment) Option {
	return func(o *options) {
		if exp != nil {
			o.exp = exp.Sub(fmt.Sprintf("cesium-%s", o.dirname))
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

func openPebbleKVDB(opts *options) (kvEngine, error) {
	pOpts := &pebble.Options{}
	if opts.memBacked {
		pOpts.FS = vfs.NewMem()
	}
	pdb, err := pebble.Open(filepath.Join(opts.dirname, "pebble"), pOpts)
	return pebbleKV{DB: pdb}, err
}

func openPersist(opts *options) Persist {
	if opts.memBacked {
		return newPersist(NewAfero(opts.dirname))
	}
	log.Info(opts.dirname)
	return newPersist(NewOS(filepath.Join(opts.dirname, "cesium")))
}
