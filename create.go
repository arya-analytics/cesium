package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/allocate"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/errutil"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/lock"
	"github.com/arya-analytics/x/queue"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type (
	createSegment    = confluence.Segment[[]createOperation]
	createStream     = query.Stream[CreateRequest, CreateResponse]
	segmentAllocator = allocate.Allocator[ChannelKey, fileKey, Segment]
	createStrategy   = query.Strategy[fileKey, createOperation, CreateRequest, CreateResponse]
	createContext    = query.Context[fileKey, createOperation, CreateRequest]
)

type createOperationSet struct {
	operation.Set[fileKey, createOperation]
}

func (c createOperationSet) Size() int { return c.Set[0].Size() }

func (c createOperationSet) SetFileKey(key fileKey) createOperation {
	panic("unimplemented")
}

func (c createOperationSet) Key() ChannelKey { return c.Set[0].Key() }

// |||||| ALLOCATOR ||||||

// |||||| STREAM ||||||

// CreateRequest is a request containing a set of segments (Segment) to write to the DB.
type CreateRequest struct {
	Segments []Segment
}

// CreateResponse contains any errors that occurred during the execution of the Create Query.
type CreateResponse struct {
	Err error
}

// Error implements the query.Response interface.
func (r CreateResponse) Error() error {
	return r.Err
}

// |||||| QUERY ||||||

type Create struct {
	query.Query
}

// WhereChannels sets the channels to acquire a lock on for creation.
// The request stream will only accept segmentKV bound to Channel with the given primary keys.
// If no keys are provided, will return an ErrInvalidQuery error.
func (c Create) WhereChannels(keys ...ChannelKey) Create {
	setChannelKeys(c, keys...)
	return c
}

// Stream opens a stream
func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, <-chan CreateResponse, error) {
	query.SetContext(c, ctx)
	s := createStream{Requests: make(chan CreateRequest), Responses: make(chan CreateResponse)}
	query.SetStream[CreateRequest, CreateResponse](c, s)
	return s.Requests, s.Responses, c.Query.QExec()
}

// |||||| QUERY FACTORY ||||||

type createFactory struct {
	exec query.Executor
}

func (c createFactory) New() Create {
	return Create{Query: query.New(c.exec)}
}

// |||||| OPERATION ||||||

type createOperation interface {
	operation.Operation[fileKey]
	allocate.Item[ChannelKey]
	SetFileKey(key fileKey) createOperation
}

type unaryCreateOperation struct {
	fileKey    fileKey
	channelKey ChannelKey
	segments   []Segment
	segmentKV  segmentKV
	stream     createStream
	ctx        context.Context
	logger     *zap.Logger
	metrics    createMetrics
	wg         *sync.WaitGroup
}

// Context implements createOperation.
func (cr unaryCreateOperation) Context() context.Context { return cr.ctx }

// FileKey implements createOperation.
func (cr unaryCreateOperation) FileKey() fileKey { return cr.fileKey }

// WriteError implements createOperation.
func (cr unaryCreateOperation) WriteError(err error) { cr.stream.Responses <- CreateResponse{Err: err} }

// Key implements allocate.Item.
func (cr unaryCreateOperation) Key() ChannelKey { return cr.channelKey }

func (cr unaryCreateOperation) SetFileKey(key fileKey) createOperation {
	cr.fileKey = key
	return cr
}

func (cr unaryCreateOperation) Size() int {
	size := 0
	for _, seg := range cr.segments {
		size += seg.Size()
	}
	return size

}

// Exec implements persist.Operation.
func (cr unaryCreateOperation) Exec(f file) {
	tft := cr.metrics.totalFlush.Stopwatch()
	tft.Start()
	c := errutil.NewCatchReadWriteSeek(f, errutil.WithHooks(cr.WriteError))
	c.Seek(0, io.SeekEnd)
	for _, s := range cr.segments {
		s.fileKey = f.Key()
		s.ChannelKey = cr.channelKey
		s.size = Size(s.Size())
		s.offset = c.Seek(0, io.SeekCurrent)
		c.Exec(func() error {
			fs := cr.metrics.dataFlush.Stopwatch()
			fs.Start()
			err := s.flushData(f)
			fs.Stop()
			return err
		})
		c.Exec(func() error {
			ks := cr.metrics.kvFlush.Stopwatch()
			ks.Start()
			err := cr.segmentKV.set(s)
			ks.Stop()
			return err
		})
	}
	cr.wg.Done()
	d := tft.Stop()
	last := cr.segments[len(cr.segments)-1]
	cr.logger.Debug(
		"completed create operation",
		zap.Any("file", f.Key()),
		zap.Time("start", cr.segments[0].Start.Time()),
		zap.Binary("key", cr.segments[0].KVKey()),
		zap.Time("end", last.Start.Time()),
		zap.Duration("duration", d),
	)
}

// ChannelKey implements batch.CreateOperation.
func (cr unaryCreateOperation) ChannelKey() ChannelKey { return cr.channelKey }

// |||||| PARSER ||||||

type createParser struct {
	skv       segmentKV
	allocator segmentAllocator
	metrics   createMetrics
	logger    *zap.Logger
}

func (cp *createParser) Parse(q query.Query, req CreateRequest) (ops []createOperation, err error) {
	fileKeys := cp.allocator.Allocate(req.Segments...)
	stream := query.GetStream[CreateRequest, CreateResponse](q)
	for i, seg := range req.Segments {
		cp.metrics.segSize.Record(seg.Size())
		ops = append(ops, unaryCreateOperation{
			fileKey:    fileKeys[i],
			channelKey: seg.ChannelKey,
			segments:   []Segment{seg},
			segmentKV:  cp.skv,
			stream:     stream,
			ctx:        q.Context(),
			logger:     cp.logger,
			metrics:    cp.metrics,
			wg:         query.WaitGroup(q),
		})
	}
	return ops, nil
}

// |||||| HOOKS ||||||

type lockAcquireHook struct {
	lock   lock.Map[ChannelKey]
	metric alamos.Duration
}

func (l lockAcquireHook) Exec(query query.Query) error {
	s := l.metric.Stopwatch()
	s.Start()
	defer s.Stop()
	return l.lock.Acquire(channelKeys(query)...)
}

type lockReleaseHook struct {
	lock   lock.Map[ChannelKey]
	metric alamos.Duration
}

func (l lockReleaseHook) Exec(query query.Query) error {
	s := l.metric.Stopwatch()
	s.Start()
	defer s.Stop()
	l.lock.Release(channelKeys(query)...)
	return nil
}

// |||||| ITERATOR ||||||

type createIterFactory struct {
	queue chan<- []createOperation
}

func (cf *createIterFactory) New(ctx createContext) (query.Iterator[CreateRequest], error) {
	return &createIterator{ctx, cf}, nil
}

type createIterator struct {
	createContext
	*createIterFactory
}

func (cp *createIterator) Next(request CreateRequest) (last bool) {
	stream := query.GetStream[CreateRequest, CreateResponse](cp.Query)
	ops, err := cp.Parser.Parse(cp.Query, request)
	if err != nil {
		stream.Responses <- CreateResponse{Err: err}
	}
	cp.WaitGroup.Add(len(ops))
	cp.queue <- ops
	return false
}

// |||||| BATCH ||||||

type createBatch struct {
	confluence.Transform[[]createOperation]
}

func (b *createBatch) batch(ctx confluence.Context, ops []createOperation) ([]createOperation, bool) {
	files := make(map[fileKey][]createOperation)
	for _, op := range ops {
		files[op.FileKey()] = append(files[op.FileKey()], op)
	}
	channels := make(map[ChannelKey]createOperationSet)
	for _, op := range ops {
		channels[op.Key()] = createOperationSet{Set: append(channels[op.Key()].Set, op)}
	}
	sets := make([]createOperation, 0, len(channels))
	for _, opSet := range channels {
		sets = append(sets, opSet)
	}
	return sets, true
}

func newCreateBatch() createSegment {
	b := &createBatch{}
	b.Transform.Transform = b.batch
	return b
}

// |||||| METRICS ||||||

// createMetrics is a collection of metrics tracking the performance and health of a Create query.
type createMetrics struct {
	// dataFlush tracks the duration it takes to flush Segment data to disk.
	dataFlush alamos.Duration
	// kvFlush tracks the duration it takes to flush Segment kv data.
	kvFlush alamos.Duration
	// totalFlush tracks the duration it takes all the operations to disk.
	// (dataFlush,kvFlush,seeks, etc.)
	totalFlush alamos.Duration
	// lockAcquire tracks the duration it takes to acquire the lock on the channels
	// that are being written to.
	lockAcquire alamos.Duration
	// lockRelease tracks the duration it takes to release the lock on the channels
	// that are being written to.
	lockRelease alamos.Duration
	// segSize tracks the Size of each Segment created.
	segSize alamos.Metric[int]
	// request tracks the total duration that the Create query is open i.e. from
	// calling Create.Stream(ctx) to the close(res) call.
	request alamos.Duration
}

const (
	// createMetricsKey is the key used to store create metrics in cesium's alamos.Experiment.
	createMetricsKey = "create"
)

func newCreateMetrics(exp alamos.Experiment) createMetrics {
	sub := alamos.Sub(exp, createMetricsKey)
	return createMetrics{
		segSize:     alamos.NewGauge[int](sub, alamos.Debug, "segSize"),
		lockAcquire: alamos.NewGaugeDuration(sub, alamos.Debug, "lockAcquireDur"),
		lockRelease: alamos.NewGaugeDuration(sub, alamos.Debug, "lockReleaseDur"),
		dataFlush:   alamos.NewGaugeDuration(sub, alamos.Debug, "dataFlushDur"),
		kvFlush:     alamos.NewGaugeDuration(sub, alamos.Debug, "kvFlushDur"),
		totalFlush:  alamos.NewGaugeDuration(sub, alamos.Debug, "totalFlushDur"),
		request:     alamos.NewGaugeDuration(sub, alamos.Debug, "requestDur"),
	}
}

// |||||| START UP |||||||

const fileCounterKey = "cs-nf"

type createConfig struct {
	// exp is used to track metrics for the Create query. See createMetrics for all the recorded values.
	exp alamos.Experiment
	// fs is the file system for writing Segment data to.
	fs fileSystem
	// kv is the key-value store for writing Segment metadata to.
	kv kv.KV
	// shutdown is used to gracefully shutdown down create operations.
	// create releases the shutdown when all Segment data has persisted to disk.
	shutdown shutdown.Shutdown
	// logger is where create operations will log their progress.
	logger *zap.Logger
	// allocate is used for setting the parameters for allocating a Segment to  afile.
	// This setting is particularly useful in environments where the maximum number of
	// file descriptors must be limited.
	allocate allocate.Config
	// persist is used for setting the parameters for persist.Persist that writes
	// Segment data to disk.
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
	// createPersistMaxRoutines is the maximum number of goroutines the create query persist.Persist can use.
	createPersistMaxRoutines = persist.DefaultNumWorkers
	// createDebounceFlushInterval is the interval at which create debounce queue will flush if the number of
	// create operations is below the threshold.
	createDebounceFlushInterval = 100 * time.Millisecond
	// createDebounceFlushThreshold is the number of requests that must be queued before create debounce queue
	// will flush.
	createDebounceFlushThreshold = 100
)

func startCreate(cfg createConfig) (query.Factory[Create], error) {

	mergeCreateConfigDefaults(&cfg)

	// strategy represents a general strategy for executing a Create query.
	// The elements of the strategy are intended to be modular and swappable
	// for the purposes of experimentation. All components of the strategy
	// must follow the interface laid out in the query.Strategy interface.
	strategy := new(createStrategy)

	metrics := newCreateMetrics(cfg.exp)
	strategy.Metrics.Request = metrics.request

	ckv, skv := channelKV{kv: cfg.kv}, segmentKV{kv: cfg.kv}

	strategy.Shutdown = cfg.shutdown

	// a kv persisted counter that tracks the number of files that a DB has created.
	// The segment allocator uses it to determine the next file to open.
	fCount, err := newFileCounter(cfg.kv, []byte(fileCounterKey))
	if err != nil {
		return nil, err
	}

	// allocator is responsible for allocating new Segments to files.
	allocator := allocate.New[ChannelKey, fileKey, Segment](fCount, allocate.Config{
		MaxDescriptors: 10,
		MaxSize:        1e9,
	})

	strategy.Parser = &createParser{
		skv:       skv,
		allocator: allocator,
		logger:    cfg.logger,
		metrics:   metrics,
	}

	// acquires and releases the locks on channels. Acquiring locks on channels simplifies
	// the implementation of the database significantly, as we can avoid needing to serialize writes to the same Channel
	// from different goroutines.
	channelLock := lock.NewMap[ChannelKey]()

	strategy.Hooks.PreAssembly = []query.Hook{
		// validates that all Channel keys provided to the Create query are valid.
		validateChannelKeysHook{ckv: ckv},
		// acquires the locks on the channels that are being written to.
		lockAcquireHook{lock: channelLock, metric: metrics.lockAcquire},
	}

	strategy.Hooks.PostExecution = []query.Hook{
		// releases the locks on the channels that are being written to.
		lockReleaseHook{lock: channelLock, metric: metrics.lockRelease},
		// closes the stream response Channel for the query, which signals to the caller that the
		// query has completed, and the Header writes are durable.
		query.CloseStreamResponseHook[CreateRequest, CreateResponse]{},
	}

	// executes the operations generated by strategy.Parser, persisting them to disk.

	// 'debounces' the execution of operations generated by strategy.Parser in order to allow for more
	// efficient batching.
	pipeline := confluence.NewPipeline[[]createOperation]()
	pipeline.Segment("queue", &queue.Debounce[createOperation]{DebounceConfig: cfg.debounce})
	pipeline.Segment("batch", newCreateBatch())
	pipeline.Segment("persist", persist.New[fileKey, createOperation](cfg.fs, cfg.persist))

	rb := pipeline.NewRouteBuilder()
	rb.Route(confluence.UnaryRouter[[]createOperation]{FromAddr: "queue", ToAddr: "batch", Capacity: 10})
	rb.Route(confluence.UnaryRouter[[]createOperation]{FromAddr: "batch", ToAddr: "persist", Capacity: 10})
	rb.RouteInletTo("queue")
	inlet := confluence.NewStream[[]createOperation](10)
	pipeline.InFrom(inlet)
	ctx := confluence.DefaultContext()
	ctx.Shutdown = cfg.shutdown
	pipeline.Flow(ctx)
	strategy.IterFactory = &createIterFactory{queue: inlet.Inlet()}

	return createFactory{exec: strategy}, nil
}
