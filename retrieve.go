package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/queue"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"io"
	"sort"
	"sync"
	"time"
)

type (
	retrieveStream       = query.Stream[retrieveRequest, RetrieveResponse]
	retrieveOperationSet = operation.Set[fileKey, retrieveOperation]
	retrieveStrategy     = query.Strategy[fileKey, retrieveOperation, retrieveRequest, RetrieveResponse]
	retrieveContext      = query.Context[fileKey, retrieveOperation, retrieveRequest]
)

// |||||| STREAM ||||||

// RetrieveResponse is a response containing segments satisfying a Retrieve Query as well as any errors
// encountered during the retrieval.
type RetrieveResponse struct {
	Err      error
	Segments []Segment
}

// Error implements the query.Response interface.
func (r RetrieveResponse) Error() error {
	return r.Err
}

type retrieveRequest struct {
	tr TimeRange
}

// |||||| QUERY ||||||

type Retrieve struct {
	query.Query
}

// WhereChannels sets the channels to retrieve data for.
// If no keys are provided, will return an ErrInvalidQuery error.
func (r Retrieve) WhereChannels(keys ...ChannelKey) Retrieve {
	setChannelKeys(r, keys...)
	return r
}

// WhereTimeRange sets the time range to retrieve data from.
func (r Retrieve) WhereTimeRange(tr TimeRange) Retrieve {
	setTimeRange(r, tr)
	return r
}

func (r Retrieve) Stream(ctx context.Context) (<-chan RetrieveResponse, error) {
	query.SetContext(r, ctx)
	s := retrieveStream{Responses: make(chan RetrieveResponse), Requests: make(chan retrieveRequest)}
	query.SetStream(r, s)
	err := r.QExec()
	if err != nil {
		return nil, err
	}
	s.Requests <- retrieveRequest{tr: TimeRangeMax}
	//close(s.Requests)
	return s.Responses, nil
}

// |||||| OPTIONS ||||||

// |||| TIME RANGE ||||

const timeRangeOptKey query.OptionKey = "tr"

func setTimeRange(q query.Query, tr TimeRange) {
	q.Set(timeRangeOptKey, tr)
}

func timeRange(q query.Query) TimeRange {
	tr, ok := q.Get(timeRangeOptKey)
	if !ok {
		return TimeRangeMax
	}
	return tr.(TimeRange)
}

// |||||| QUERY FACTORY ||||||

type retrieveFactory struct {
	exec query.Executor
}

func (r retrieveFactory) New() Retrieve {
	return Retrieve{Query: query.New(r.exec)}
}

// ||||||| OPERATION ||||||

type retrieveOperation struct {
	seg      Segment
	stream   retrieveStream
	wg       *sync.WaitGroup
	ctx      context.Context
	dataRead alamos.Duration
}

// Context implements persist.Operation.
func (ro retrieveOperation) Context() context.Context {
	return ro.ctx
}

// FileKey implements persist.Operation.
func (ro retrieveOperation) FileKey() fileKey {
	return ro.seg.fileKey
}

// SendError implements persist.Operation.
func (ro retrieveOperation) WriteError(err error) {
	ro.stream.Responses <- RetrieveResponse{Err: err}
}

// Exec implements persist.Operation.
func (ro retrieveOperation) Exec(f file) {
	defer ro.wg.Done()
	s := ro.dataRead.Stopwatch()
	s.Start()
	b := make([]byte, ro.seg.size)
	if _, err := f.ReadAt(b, ro.seg.offset); err != nil {
		if err == io.EOF {
			panic("retrieve operation: encountered unexpected EOF. this is a bug.")
		}
		ro.WriteError(err)
	}
	ro.seg.Data = b
	s.Stop()
	ro.stream.Responses <- RetrieveResponse{Segments: []Segment{ro.seg}}
}

// Offset implements batch.RetrieveOperation.
func (ro retrieveOperation) Offset() int64 {
	return ro.seg.offset
}

// |||||| PARSER ||||||

type retrieveParser struct {
	ckv     channelKV
	skv     segmentKV
	logger  *zap.Logger
	metrics retrieveMetrics
}

func (rp *retrieveParser) Parse(q query.Query, req retrieveRequest) (ops []retrieveOperation, err error) {
	keys := channelKeys(q)
	queryRange := timeRange(q)
	requestRange := req.tr

	last := false
	if requestRange.End >= queryRange.End {
		last = true
	}

	requestRange = queryRange.Bound(requestRange)

	stream := query.GetStream[retrieveRequest, RetrieveResponse](q)

	rp.logger.Debug("retrieving segments",
		zap.Int("count", len(keys)),
		zap.Time("from", queryRange.Start.Time()),
		zap.Time("to", queryRange.End.Time()),
	)

	for _, key := range keys {
		kvs := rp.metrics.kvRetrieve.Stopwatch()
		kvs.Start()
		segments, err := rp.skv.filter(requestRange, key)
		kvs.Stop()
		if err != nil {
			return ops, err
		}
		rp.metrics.segCount.Record(len(segments))
		for _, seg := range segments {
			rp.metrics.segSize.Record(seg.Size())
			ops = append(ops,
				retrieveOperation{
					seg:      seg,
					stream:   stream,
					wg:       query.WaitGroup(q),
					dataRead: rp.metrics.dataRead,
					ctx:      q.Context(),
				},
			)
		}
	}
	if last {
		return ops, io.EOF
	}
	return ops, nil
}

// |||||| EXECUTOR ||||||

type retrieveIteratorFactory struct {
	queue    chan<- []retrieveOperation
	shutdown shutdown.Shutdown
	logger   *zap.Logger
}

func (rp *retrieveIteratorFactory) New(ctx retrieveContext) (query.Iterator[retrieveRequest], error) {
	return &retrieveIterator{ctx, rp}, nil
}

type retrieveIterator struct {
	retrieveContext
	*retrieveIteratorFactory
}

func (r *retrieveIterator) Next(request retrieveRequest) (last bool) {
	stream := query.GetStream[retrieveRequest, RetrieveResponse](r.Query)
	ops, err := r.Parser.Parse(r.Query, request)
	if err == io.EOF {
		last = true
	} else if err != nil {
		stream.Responses <- RetrieveResponse{Err: err}
	}
	r.WaitGroup.Add(len(ops))
	r.queue <- ops
	return last
}

// ||||||| BATCH |||||||

type retrieveBatch struct{}

func (r *retrieveBatch) Exec(ops []retrieveOperation) []retrieveOperationSet {
	files := make(map[fileKey][]retrieveOperation)
	for _, op := range ops {
		files[op.FileKey()] = append(files[op.FileKey()], op)
	}
	sets := make([]retrieveOperationSet, 0, len(files))
	for _, ops := range files {
		sort.Slice(ops, func(i, j int) bool {
			return ops[i].Offset() < ops[j].Offset()
		})
		sets = append(sets, ops)
	}
	return sets
}

// |||||| METRICS ||||||

type retrieveMetrics struct {
	// kvRetrieve is the time spent retrieving Segment metadata from key-value storage.
	kvRetrieve alamos.Duration
	// dataRead is the duration spent reading Segment data from disk.
	dataRead alamos.Duration
	// segSize tracks the size of each Segment retrieved.
	segSize alamos.Metric[int]
	// segCount tracks the number of segments retrieved.
	segCount alamos.Metric[int]
	// request tracks the total duration that the Retrieve query is open i.e. from calling Retrieve.Stream(ctx) to
	// an internal close(res) call that represents the query is complete.
	request alamos.Duration
}

const (
	// retrieveMetricsKey is the key used to store retrieve metrics in cesium's alamos.Experiment.
	retrieveMetricsKey = "retrieve"
)

func newRetrieveMetrics(exp alamos.Experiment) retrieveMetrics {
	sub := alamos.Sub(exp, retrieveMetricsKey)
	return retrieveMetrics{
		kvRetrieve: alamos.NewGaugeDuration(sub, "kvRetrieveDur"),
		dataRead:   alamos.NewGaugeDuration(sub, "dataReadDur"),
		segSize:    alamos.NewGauge[int](sub, "segSize"),
		segCount:   alamos.NewGauge[int](sub, "segCount"),
		request:    alamos.NewGaugeDuration(sub, "requestDur"),
	}
}

type retrieveConfig struct {
	// exp is used to track metrics for the Retrieve query. See retrieveMetrics for more.
	exp alamos.Experiment
	// fs is the file system for reading Segment data from.
	fs fileSystem
	// kv is the key-value store for reading Segment metadata from.
	kv kv.KV
	// shutdown is used to gracefully shutdown down retrieve operations.
	// retrieve releases the shutdown when all queries have been served.
	shutdown shutdown.Shutdown
	// logger is where retrieve operations will log their progress.
	logger *zap.Logger
	// debounce sets the debounce parameters for retrieve operations.
	// this is mostly here for optimizing performance under varied conditions.
	debounce queue.DebounceConfig
	// persist used for setting the parameters for persist.Persist thar reads
	// segment data from disk.
	persist persist.Config
}

func mergeRetrieveConfigDefaults(cfg *retrieveConfig) {

	// |||||| PERSIST ||||||

	if cfg.persist.NumWorkers == 0 {
		cfg.persist.NumWorkers = retrievePersistMaxRoutines
	}
	if cfg.persist.Shutdown == nil {
		cfg.persist.Shutdown = cfg.shutdown
	}
	if cfg.persist.Logger == nil {
		cfg.persist.Logger = cfg.logger
	}

	// |||||| DEBOUNCE ||||||

	if cfg.debounce.Interval == 0 {
		cfg.debounce.Interval = retrieveDebounceFlushInterval
	}
	if cfg.debounce.Threshold == 0 {
		cfg.debounce.Threshold = retrieveDebounceFlushThreshold
	}
	if cfg.debounce.Shutdown == nil {
		cfg.debounce.Shutdown = cfg.shutdown
	}
	if cfg.debounce.Logger == nil {
		cfg.debounce.Logger = cfg.logger
	}
}

const (
	// retrievePersistMaxRoutines is the maximum number of goroutines the retrieve query persist.Persist can use.
	retrievePersistMaxRoutines = persist.DefaultNumWorkers
	// retrieveDebounceFlushInterval is the interval at which retrieve debounce queue will flush if the number of
	// retrieve operations is below the threshold.
	retrieveDebounceFlushInterval = 100 * time.Millisecond
	// retrieveDebounceFlushThreshold is the number of retrieve operations that must be in the debounce queue before
	// it flushes
	retrieveDebounceFlushThreshold = 10
)

func startRetrieve(cfg retrieveConfig) (query.Factory[Retrieve], error) {

	mergeRetrieveConfigDefaults(&cfg)

	// strategy represents a general strategy for executing a Retrieve query.
	// The elements of the strategy are intended to be modular and swappable for
	// the purposes of experimentation. All components of the strategy must
	// follow the interface laid out in the query.Strategy interface.
	strategy := new(retrieveStrategy)

	metrics := newRetrieveMetrics(cfg.exp)
	strategy.Metrics.Request = metrics.request

	ckv := channelKV{kv: cfg.kv}

	strategy.Shutdown = cfg.shutdown

	strategy.Parser = &retrieveParser{
		ckv:     ckv,
		skv:     segmentKV{kv: cfg.kv},
		logger:  cfg.logger,
		metrics: metrics,
	}

	strategy.Hooks.PreAssembly = []query.Hook{
		// validates that all channel keys provided to the Retrieve query are valid.
		validateChannelKeysHook{ckv: ckv},
	}

	strategy.Hooks.PostExecution = []query.Hook{
		// closes the retrieve response channel for the query, which signals to the caller that the query has completed,
		// and all segments have been sent through the response channel.
		query.CloseStreamResponseHook[retrieveRequest, RetrieveResponse]{},
	}

	// executes the operations generated by strategy.Parser, reading them from disk.
	pst := persist.New[fileKey, retrieveOperationSet](cfg.fs, cfg.persist)

	// 'debounces' the execution of operations generated by strategy.Parser in order to allow for more efficient
	// operation batching.
	q := &queue.Debounce[retrieveOperation]{
		In:     make(chan []retrieveOperation),
		Out:    make(chan []retrieveOperation),
		Config: cfg.debounce,
	}
	q.Start()

	// batches operations from the debounced queue into sequential operations on the same file.
	batchPipe := operation.PipeTransform[
		fileKey,
		retrieveOperation,
		retrieveOperationSet,
	](q.Out, cfg.shutdown, new(retrieveBatch))

	pst.Pipe(batchPipe)

	// opens new iterators for generating retrieve operations for the RetrieveRequest piped in via q.In.
	strategy.IterFactory = &retrieveIteratorFactory{
		queue:    q.In,
		shutdown: cfg.shutdown,
		logger:   cfg.logger,
	}

	return retrieveFactory{exec: strategy}, nil
}
