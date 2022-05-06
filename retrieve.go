package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/alamos"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/cesium/internal/queue"
	"github.com/arya-analytics/cesium/shut"
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

type RetrieveResponse struct {
	Err      error
	Segments []Segment
}

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
func (ro retrieveOperation) SendError(err error) {
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
		ro.SendError(err)
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
	if requestRange.End.After(queryRange.End) {
		last = true
	}

	requestRange = queryRange.Bound(requestRange)

	stream := query.GetStream[retrieveRequest, RetrieveResponse](q)

	rp.logger.Debug("retrieving segments",
		zap.Int("count", len(keys)),
		zap.Time("from", queryRange.Start.Time()),
		zap.Time("to", queryRange.End.Time()),
		zap.Binary("prefix", kv.CompositeKey(segmentKVPrefix, keys[0])),
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

type retrieveIteratorProxy struct {
	queue    chan<- []retrieveOperation
	shutdown shut.Shutdown
	logger   *zap.Logger
}

func (rp *retrieveIteratorProxy) Open(ctx retrieveContext) (query.Iterator[retrieveRequest], error) {
	return &retrieveIterator{ctx, rp}, nil
}

type retrieveIterator struct {
	retrieveContext
	*retrieveIteratorProxy
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
	kvRetrieve alamos.Duration
	dataRead   alamos.Duration
	segSize    alamos.Metric[int]
	segCount   alamos.Metric[int]
}

func startRetrievePipeline(fs fileSystem, kve kv.KV, opts *options, sd shut.Shutdown) (query.Factory[Retrieve], error) {
	exp := alamos.Sub(opts.exp, "retrieve")
	metrics := retrieveMetrics{
		kvRetrieve: alamos.NewGaugeDuration(exp, "kvRetrieve"),
		dataRead:   alamos.NewGaugeDuration(exp, "dataRead"),
		segSize:    alamos.NewGauge[int](exp, "segSize"),
		segCount:   alamos.NewGauge[int](exp, "segCount"),
	}

	ckv := channelKV{kv: kve}

	strategy := new(retrieveStrategy)
	strategy.Shutdown = sd
	strategy.Parser = &retrieveParser{
		ckv:     ckv,
		skv:     segmentKV{kv: kve},
		logger:  opts.logger,
		metrics: metrics,
	}
	strategy.Hooks.PreAssembly = append(
		strategy.Hooks.PreAssembly,
		validateChannelKeysHook{ckv: ckv},
	)
	strategy.Hooks.PostExecution = append(
		strategy.Hooks.PostExecution,
		query.CloseStreamResponseHook[retrieveRequest, RetrieveResponse]{},
	)

	pst := persist.New[fileKey, retrieveOperationSet](fs, 50, sd, opts.logger)
	q := &queue.Debounce[retrieveOperation]{
		In:        make(chan []retrieveOperation),
		Out:       make(chan []retrieveOperation),
		Shutdown:  sd,
		Interval:  100 * time.Millisecond,
		Threshold: 50,
		Logger:    opts.logger,
	}
	q.Start()

	batchPipe := operation.PipeTransform[fileKey, retrieveOperation, retrieveOperationSet](q.Out, sd, new(retrieveBatch))
	operation.PipeExec[fileKey, retrieveOperationSet](batchPipe, pst, sd)

	strategy.IterProxy = &retrieveIteratorProxy{
		queue:    q.In,
		shutdown: sd,
		logger:   opts.logger,
	}

	return retrieveFactory{exec: strategy}, nil
}
