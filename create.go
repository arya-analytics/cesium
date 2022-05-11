package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/alamos"
	"github.com/arya-analytics/cesium/internal/allocate"
	"github.com/arya-analytics/cesium/internal/errutil"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/lock"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/cesium/internal/queue"
	"github.com/arya-analytics/cesium/shut"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type (
	createStream       = query.Stream[CreateRequest, CreateResponse]
	segmentAllocator   = allocate.Allocator[ChannelKey, fileKey, Segment]
	createOperationSet = operation.Set[fileKey, createOperation]
	createStrategy     = query.Strategy[fileKey, createOperation, CreateRequest, CreateResponse]
	createContext      = query.Context[fileKey, createOperation, CreateRequest]
)

// |||||| STREAM ||||||

// CreateRequest is a request containing a set of segmentKV to write to the DB.
type CreateRequest struct {
	Segments []Segment
}

// CreateResponse contains any errors that occurred during the execution of the Create query.Query.
type CreateResponse struct {
	Err error
}

// Error implements the Response interface.
func (r CreateResponse) Error() error {
	return r.Err
}

// |||||| QUERY ||||||

type Create struct {
	query.Query
}

// WhereChannels sets the channels to acquire a lock on for creation.
// The request stream will only accept segmentKV bound to channel with the given primary keys.
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

type createOperation struct {
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

// Context implements persist.Operation.
func (cr createOperation) Context() context.Context {
	return cr.ctx
}

// FileKey implements persist.Operation.
func (cr createOperation) FileKey() fileKey {
	return cr.fileKey
}

// SendError implements persist.Operation.
func (cr createOperation) SendError(err error) {
	cr.stream.Responses <- CreateResponse{Err: err}
}

// Exec implements persist.Operation.
func (cr createOperation) Exec(f file) {
	tft := cr.metrics.totalFlush.Stopwatch()
	tft.Start()
	c := errutil.NewCatchReadWriteSeek(f, errutil.WithHooks(cr.SendError))
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
func (cr createOperation) ChannelKey() ChannelKey {
	return cr.channelKey
}

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
		ops = append(ops, createOperation{
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

type createIteratorProxy struct {
	queue chan<- []createOperation
}

func (cf *createIteratorProxy) Open(ctx createContext) (query.Iterator[CreateRequest], error) {
	return &createIterator{ctx, cf}, nil
}

type createIterator struct {
	createContext
	*createIteratorProxy
}

func (cp *createIterator) Next(request CreateRequest) bool {
	stream := query.GetStream[CreateRequest, CreateResponse](cp.Query)
	ops, err := cp.Parser.Parse(cp.Query, request)
	if err != nil {
		stream.Responses <- CreateResponse{Err: err}
	}
	cp.WaitGroup.Add(len(ops))
	cp.queue <- ops
	return true
}

// |||||| BATCH ||||||

type createBatch struct{}

func (b *createBatch) Exec(ops []createOperation) []createOperationSet {
	files := make(map[fileKey][]createOperation)
	for _, op := range ops {
		files[op.fileKey] = append(files[op.fileKey], op)
	}
	channels := make(map[ChannelKey]createOperationSet)
	for _, op := range ops {
		channels[op.ChannelKey()] = append(channels[op.ChannelKey()], op)
	}
	sets := make([]createOperationSet, 0, len(channels))
	for _, opSet := range channels {
		sets = append(sets, opSet)
	}
	return sets
}

// |||||| METRICS ||||||

type createMetrics struct {
	dataFlush   alamos.Duration
	kvFlush     alamos.Duration
	totalFlush  alamos.Duration
	lockAcquire alamos.Duration
	lockRelease alamos.Duration
	segSize     alamos.Metric[int]
	request     alamos.Duration
}

// |||||| START UP |||||||

func startCreatePipeline(fs fileSystem, kve kv.KV, opts *options, sd shut.Shutdown) (query.Factory[Create], error) {
	strategy := new(createStrategy)
	exp := alamos.Sub(opts.exp, "create")
	metrics := createMetrics{
		segSize:     alamos.NewGauge[int](exp, "segSize"),
		lockAcquire: alamos.NewGaugeDuration(exp, "lockAcquireTime"),
		lockRelease: alamos.NewGaugeDuration(exp, "lockReleaseTime"),
		dataFlush:   alamos.NewGaugeDuration(exp, "dataFlushTime"),
		kvFlush:     alamos.NewGaugeDuration(exp, "kvFlushTime"),
		totalFlush:  alamos.NewGaugeDuration(exp, "totalFlushTime"),
		request:     alamos.NewGaugeDuration(exp, "requestTime"),
	}
	strategy.Metrics.Request = metrics.request

	ckv, skv := channelKV{kv: kve}, segmentKV{kv: kve}

	strategy.Shutdown = sd

	counter, err := kv.NewPersistedCounter(kve, []byte("cesium-nextFile"))
	if err != nil {
		return nil, err
	}

	nextFile := &fileCounter{PersistedCounter: *counter}

	allocator := allocate.New[ChannelKey, fileKey, Segment](nextFile, allocate.Config{
		MaxDescriptors: 10,
		MaxSize:        1e9,
	})

	strategy.Parser = &createParser{
		skv:       skv,
		allocator: allocator,
		logger:    opts.logger,
		metrics:   metrics,
	}

	channelLock := lock.NewMap[ChannelKey]()
	strategy.Hooks.PreAssembly = append(
		strategy.Hooks.PreAssembly,
		validateChannelKeysHook{ckv: ckv},
		lockAcquireHook{lock: channelLock, metric: metrics.lockAcquire},
	)
	strategy.Hooks.PostExecution = append(
		strategy.Hooks.PostExecution,
		lockReleaseHook{lock: channelLock, metric: metrics.lockRelease},
		query.CloseStreamResponseHook[CreateRequest, CreateResponse]{},
	)

	pst := persist.New[fileKey, createOperationSet](fs, 50, sd, opts.logger)
	q := &queue.Debounce[createOperation]{
		In:        make(chan []createOperation),
		Out:       make(chan []createOperation),
		Shutdown:  sd,
		Interval:  100 * time.Millisecond,
		Threshold: 50,
		Logger:    opts.logger,
	}
	q.Start()

	batchPipe := operation.PipeTransform[fileKey, createOperation, createOperationSet](q.Out, sd, &createBatch{})
	operation.PipeExec[fileKey, createOperationSet](batchPipe, pst, sd)
	strategy.IterProxy = &createIteratorProxy{queue: q.In}

	return createFactory{exec: strategy}, nil
}
