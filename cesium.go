package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/shutdown"
)

var (
	ErrNotFound = query.ErrNotFound
)

type (
	Query = query.Query
)

type DB interface {

	// NewCreate opens a new Create query that is used for writing data to the DB. A simple synchronous create query
	// looks like the following:
	//
	//      // Open the DB
	//  	ctx := context.Background()
	//		db := cesium.Open("", cesium.MemBacked())
	//
	//      // Create a new channel
	//      ch, err := cesium.NewCreateChannel().WithType(cesium.Float64).WithRate(5 * cesium.Hz).QExec(ctx)
	//		if err != nil {
	// 	    	 logger.Fatal(err)
	//		}
	//
	//	    // Create a new Segment to write. If you don't know what segments are, check out the Segment documentation.
	//      segments := cesium.Segment{
	//    		ChannelKey: ch.Pk,
	//          Start: cesium.Now(),
	//          Data: cesium.MarshalFloat64([]{1.0, 2.0, 3.0})
	//		}
	//
	//		// Open the query
	//		// db.syncExec is a helper that turns a typically async write into an acknowledged, syncExec write.
	//	    err := db.syncExec(ctx, db.NewCreate().WhereChannels(ch.Pk), []Segment{segments})
	//		if err != nil {
	//			logger.Fatal(err)
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
	//		// Write the segment to the Create Request Stream.
	//      req <- segments
	//
	//		// Close the Request Stream.
	//		close(req)
	//
	//		// Wait for the Create query to acknowledge all writes.
	// 		// The Create query will close the channel when all written segments are durable.
	// 		for resV := range res {
	//			if resV.err != nil {
	//				logger.Fatal(resV.err)
	//			}
	//		}
	//
	//		// Do what you want, but remember to close the database when done.
	//
	// Although waiting for the response channel to close is a common pattern for Create queries, it is not required.
	// cesium will ensure all writes are acknowledged upon DB.Close.
	NewCreate() Create

	// NewRetrieve opens a new Retrieve query that is used for retrieving data from the DB. A simple, synchronous
	// retrieve query looks like the following:
	//
	// 	 	// Open the DB, create a channel, and write some data to it. See NewCreate for details.
	//
	//		// Open the Retrieve query, and write the results into resSeg.
	//		// DB.Sync is a helper that turns a async read into syncExec read.
	//		// If you don't know what a Segment is, check out the Segment documentation.
	//		var resSeg []Segment
	//		err := db.syncExec(ctx, db.NewRetrieve().WhereTimeRange(cesium.TimeRangeMax).WhereChannels(ch.Pk), &resSeg)
	//		if err != nil {
	//			logger.Fatal(err)
	//		}
	//
	// The above example retrieves all data from the channel and binds it into resSeg. It's possible to retrieve a subset
	// of data by time range by using the Retrieve.WhereTimeRange method.
	//
	// Notes on Segmentation of Data:
	//
	//		Retrieve results returned as segments (Segment). The segments are not guaranteed to be
	//		in chronological order. This is a performance optimization to allow for more efficient data retrieval.
	//
	//	 	Segments are also not guaranteed to be contiguous. Because Create pattern cesium uses, it's possible
	// 		to leave gaps between segments (these represent times when that particular sensor was inactive).
	//
	//      Retrieve also DOES NOT return partial Segments ie if a query asks for time range 0 to 10, and Segment A
	//		contains the data from time 0 to 6, and Segment B contains the data from 6 to 12, ALL Segment A will be returned
	// 		and ALL Segment B will be returned (meaning the time range is 0 to 12).
	//		Changes are in progress to allow for partial Segment returns.
	//
	// Retrieve will return cesium.ErrNotFound if the query returns no data.
	//
	// Asynchronous Retrieve queries are the default in cesium. This allows for network optimization (i.e. send the data
	// across the network as you read more data from IO). However, they are a more complex to write.
	// See the following example:
	//
	// 		// Assuming DB is opened and two Segment have been created for a channel with LKey channelKey. See NewCreate for details.
	//      // Start the retrieve query. See Retrieve.Stream for details on what each return value does.
	// 		// To cancel a query before it completes, cancel the Context provided to Retrieve.Stream.
	// 		ctx, cancel := Context.WithCancel(Context.Background())
	//		res, err := db.NewRetrieve().WhereTimeRange(cesium.TimeRangeMax).WhereChannels(channelKey).Stream(ctx)
	//
	//      var resSeg []Segment
	//		// Retrieve will close the channel when done.
	// 		for _, resV := range res {
	//			if resV.Err != nil {
	//				logger.Fatal(resV.Err)
	//			}
	//			resSeg = append(resSeg, resV.Segments...)
	//		}
	//
	//      // do what you want with the data, just remember to close the database when done.
	NewRetrieve() Retrieve

	// NewCreateChannel opens a new CreateChannel query that is used for creating a new channel in the DB.
	// Creating a channel is simple:
	//
	//		// Open the DB
	//		ctx := context.Background()
	//		db := cesium.Open("", cesium.MemBacked())
	//
	//		// Create a channel
	//		ch, err := cesium.NewCreateChannel().
	//				WithType(cesium.Float64).
	//				WithRate(5 * cesium.Hz).
	//				QExec(ctx)
	//		if err != nil {
	//			logger.Fatal(err)
	//		}
	//		fmt.Println(ch.Key)
	//		// output:
	//		//  1
	//
	// See the Channel documentation for details on what a Channel is, and the CreateChannel documentation
	// for available options for creating a channel.
	NewCreateChannel() CreateChannel

	// NewRetrieveChannel opens a new RetrieveChannel query that is used for retrieving information about a Channel
	// from the DB. Retrieving a channel is simple:
	//
	// 		// Assuming DB is opened and a channel with Key 1 has been created. See NewCreateChannel for details.
	//
	//		// Retrieve the channel.
	//		ch, err := cesium.NewRetrieveChannel().WhereKey(1).QExec(ctx)
	//		if err != nil {
	//			logger.Fatal(err)
	//		}
	//		fmt.Println(ch.Key)
	//		// output:
	//		//  1
	NewRetrieveChannel() RetrieveChannel

	// Sync is a utility that executes a query synchronously. It is useful for operations that require all data to be
	// returned	before continuing.
	//
	// Sync only supports Create and Retrieve queries, as CreateChannel and RetrieveChannel are already synchronous.
	// In the case of a Create query, the 'segments' arg represents the data to write to the DB. A Retrieve query
	// will do the reverse, binding returned data to the 'segments' arg.
	//
	// For examples on how to use Sync, see the documentation for NewCreate and NewRetrieve.
	Sync(ctx context.Context, query Query, segments *[]Segment) error

	// Close closes the DB. Close ensures that all queries are complete and all data is flushed to disk.
	// Close will block until all queries are finished, so make sure to stop any running queries
	// before calling.
	Close() error
}

type db struct {
	kv              kv.KV
	shutdown        *shutdown.Group
	create          query.Factory[Create]
	retrieve        query.Factory[Retrieve]
	createChannel   query.Factory[CreateChannel]
	retrieveChannel query.Factory[RetrieveChannel]
}

// NewCreate implements DB.
func (d *db) NewCreate() Create {
	return d.create.New()
}

// NewRetrieve implements DB.
func (d *db) NewRetrieve() Retrieve {
	return d.retrieve.New()
}

// NewCreateChannel implements DB.
func (d *db) NewCreateChannel() CreateChannel {
	return d.createChannel.New()
}

// NewRetrieveChannel implements DB.
func (d *db) NewRetrieveChannel() RetrieveChannel {
	return d.retrieveChannel.New()
}

// Sync implements DB.
func (d *db) Sync(ctx context.Context, query Query, seg *[]Segment) error {
	return syncExec(ctx, query, seg)
}

// Close implements DB.
func (d *db) Close() error {
	if err := d.shutdown.Sequential(); err != nil {
		return err
	}
	return d.kv.Close()
}
