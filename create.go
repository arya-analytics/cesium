package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/allocate"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/core"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/confluence"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/lock"
	"github.com/arya-analytics/x/query"
	"github.com/arya-analytics/x/queue"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"sync"
	"time"
)

type createSegment = confluence.Segment[[]createOperation]

// |||||| STREAM ||||||

// CreateRequest is a request containing a set of segments (segment) to write to the DB.
type CreateRequest struct {
	Segments []segment.Segment
}

// CreateResponse contains any errors that occurred during the execution of the Create Query.
type CreateResponse struct {
	Error error
}

// |||||| QUERY ||||||

type Create struct {
	query.Query
	ops     confluence.Inlet[[]createOperation]
	lock    lock.Map[channel.Key]
	kv      kvx.KV
	logger  *zap.Logger
	metrics createMetrics
}

// WhereChannels sets the channels to acquire a lock on for creation.
// The request stream will only accept segmentKV bound to channel with the given primary keys.
// If no keys are provided, will return an ErrInvalidQuery error.
func (c Create) WhereChannels(keys ...channel.Key) Create { channel.SetKeys(c, keys...); return c }

// Stream opens a stream
func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, <-chan CreateResponse, error) {
	query.SetContext(c, ctx)
	keys := channel.GetKeys(c)
	if err := c.lock.Acquire(keys...); err != nil {
		return nil, nil, err
	}
	defer c.lock.Release(keys...)

	channels, err := kv.NewChannel(c.kv).Get(keys...)
	if err != nil {
		return nil, nil, err
	}

	if len(channels) != len(keys) {
		return nil, nil, NotFound
	}

	chanMap := make(map[channel.Key]channel.Channel)
	for _, ch := range channels {
		chanMap[ch.Key] = ch
	}

	requests := confluence.NewStream[CreateRequest](10)
	responses := confluence.NewStream[CreateResponse](10)
	header := kv.NewHeader(c.kv)

	go func() {
		requestDur := c.metrics.request.Stopwatch()
		requestDur.Start()
		wg := &sync.WaitGroup{}
		defer func() {
			wg.Wait()
			close(responses.Inlet())
			requestDur.Stop()
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case req, ok := <-requests.Outlet():
				if !ok {
					return
				}
				var ops []createOperation
				for _, seg := range req.Segments {
					op := createOperationUnary{
						ctx:     ctx,
						seg:     seg.Sugar(chanMap[seg.ChannelKey]),
						logger:  c.logger,
						kv:      header,
						metrics: c.metrics,
						wg:      wg,
					}
					c.metrics.segSize.Record(int(op.seg.UnboundedSize()))
					op.OutTo(responses)
					ops = append(ops, op)
				}
				wg.Add(len(ops))
				c.ops.Inlet() <- ops
			}
		}
	}()
	return requests.Inlet(), responses.Outlet(), nil
}

// |||||| QUERY FACTORY ||||||

type createFactory struct {
	lock    lock.Map[channel.Key]
	kv      kvx.KV
	logger  *zap.Logger
	header  *kv.Header
	metrics createMetrics
	ops     confluence.Inlet[[]createOperation]
}

// New implements the query.Factory interface.
func (c createFactory) New() Create {
	return Create{
		Query:   query.New(),
		kv:      c.kv,
		logger:  c.logger,
		metrics: c.metrics,
		ops:     c.ops,
		lock:    c.lock,
	}
}

// |||||| START UP |||||||

type createConfig struct {
	// exp is used to track metrics for the Create query. See createMetrics for all the recorded values.
	exp alamos.Experiment
	// fs is the file system for writing segment data to.
	fs core.FS
	// kv is the key-value store for writing segment metadata to.
	kv kvx.KV
	// shutdown is used to gracefully shutdown down create operations.
	// create releases the shutdown when all segment data has persisted to disk.
	shutdown shutdown.Shutdown
	// logger is where create operations will log their progress.
	logger *zap.Logger
	// allocate is used for setting the parameters for allocating a segment to  afile.
	// This setting is particularly useful in environments where the maximum number of
	// file descriptors must be limited.
	allocate allocate.Config
	// persist is used for setting the parameters for persist.Persist that writes
	// segment data to disk.
	persist persist.Config
	// debounce sets the debounce parameters for create operations.
	// this is mostly here for optimizing performance under varied conditions.
	debounce queue.DebounceConfig
}

func mergeCreateConfigDefaults(cfg *createConfig) {

	// |||||| ALLOCATION ||||||

	if cfg.allocate.MaxSize == 0 {
		cfg.allocate.MaxSize = maxFileSize
	}
	if cfg.allocate.MaxDescriptors == 0 {
		cfg.allocate.MaxDescriptors = maxFileDescriptors
	}

	// |||||| PERSIST ||||||

	if cfg.persist.NumWorkers == 0 {
		cfg.persist.NumWorkers = createPersistMaxRoutines
	}
	if cfg.persist.Shutdown == nil {
		cfg.persist.Shutdown = cfg.shutdown
	}
	if cfg.persist.Logger == nil {
		cfg.persist.Logger = cfg.logger
	}

	// |||||| DEBOUNCE ||||||

	if cfg.debounce.Interval == 0 {
		cfg.debounce.Interval = createDebounceFlushInterval
	}
	if cfg.debounce.Threshold == 0 {
		cfg.debounce.Threshold = createDebounceFlushThreshold
	}
}

const (
	// createPersistMaxRoutines is the maximum number of goroutines the create
	// query persist.Persist can use.
	createPersistMaxRoutines = persist.DefaultNumWorkers
	// createDebounceFlushInterval is the interval at which create debounce
	// queue will flush if the number of create operations is below the threshold.
	createDebounceFlushInterval = 100 * time.Millisecond
	// createDebounceFlushThreshold is the number of operations that must be queued
	//before create debounce queue will flush.
	createDebounceFlushThreshold = 100
	// fileCounterKey is the key for the counter that keeps track of the number of files
	// the DB has created.
	fileCounterKey = "cesium.nextFile"
)

func startCreate(cfg createConfig) (query.Factory[Create], error) {

	mergeCreateConfigDefaults(&cfg)

	// a kv persisted counter that tracks the number of files that a DB has created.
	// The segment allocator uses it to determine the next file to open.
	fCount, err := newFileCounter(cfg.kv, []byte(fileCounterKey))
	if err != nil {
		return nil, err
	}

	// acquires and releases the locks on channels. Acquiring locks on channels simplifies
	// the implementation of the database significantly, as we can avoid needing to
	// serialize writes to the same channel from different goroutines.
	channelLock := lock.NewMap[channel.Key]()

	pipeline := confluence.NewPipeline[[]createOperation]()

	// allocator allocates segments to files.
	pipeline.Segment("allocator", newAllocator(fCount, cfg.allocate))

	// queue 'debounces' operations so that they can be flushed to disk in efficient
	// batches.
	pipeline.Segment("queue", &queue.Debounce[createOperation]{
		DebounceConfig: cfg.debounce,
	})

	// batch groups operations into batches that are more efficient upon retrieval.
	pipeline.Segment("batch", newCreateBatch())

	// persist executes batched operations to disk.
	pipeline.Segment("persist", persist.New[core.FileKey, createOperation](cfg.fs, cfg.persist))

	rb := pipeline.NewRouteBuilder()

	rb.Route(confluence.UnaryRouter[[]createOperation]{
		FromAddr: "allocator",
		ToAddr:   "queue",
		Capacity: 1,
	})

	rb.Route(confluence.UnaryRouter[[]createOperation]{
		FromAddr: "queue",
		ToAddr:   "batch",
		Capacity: 1,
	})

	rb.Route(confluence.UnaryRouter[[]createOperation]{
		FromAddr: "batch",
		ToAddr:   "persist",
		Capacity: 1,
	})

	rb.RouteInletTo("allocator")

	// inlet is the inlet for all create operations that executed on disk.
	inlet := confluence.NewStream[[]createOperation](1)

	pipeline.InFrom(inlet)

	ctx := confluence.DefaultContext()
	ctx.Shutdown = cfg.shutdown

	pipeline.Flow(ctx)

	return createFactory{
		lock:    channelLock,
		kv:      cfg.kv,
		logger:  cfg.logger,
		metrics: newCreateMetrics(cfg.exp),
		ops:     inlet,
	}, rb.Error()
}
