package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/internal/query"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/queue"
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/telem"
	"go.uber.org/zap"
	"io"
	"sort"
	"sync"
	"time"
)

type retrieveSegment = confluence.Segment[[]retrieveOperation]

// |||||| OPERATION ||||||

type retrieveOperation interface {
	operation.Operation[fileKey]
	Offset() int64
}

type retrieveOperationSet struct {
	operation.Set[fileKey, retrieveOperation]
}

func (s retrieveOperationSet) Offset() int64 { return s.Set[0].Offset() }

// |||||| STREAM ||||||

// RetrieveResponse is a response containing segments satisfying a Retrieve Query as well as any errors
// encountered during the retrieval.
type RetrieveResponse struct {
	Err      error
	Segments []Segment
}

// Error implements the query.Response interface.
func (r RetrieveResponse) Error() error { return r.Err }

// |||||| QUERY ||||||

type Retrieve struct {
	query.Query
	ops     confluence.Inlet[[]retrieveOperation]
	kve     kv.KV
	rng     telem.TimeRange
	channel Channel
	ckv     channelKV
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
	stream := confluence.NewStream[RetrieveResponse](10)
	iter, err := r.NewIter()
	if err != nil {
		return nil, err
	}
	iter.OutTo(stream)
	go iter.Exhaust()
	return stream.Outlet(), nil
}

func (r Retrieve) NewIter() (StreamIterator, error) {
	ck := channelKeys(r)
	c, err := r.ckv.get(ck[0])
	if err != nil {
		return nil, err
	}
	return &streamIterator{
		kvIterator: *newSegmentKVIterator(c, timeRange(r), r.kve),
		ops:        r.ops,
		wg:         &sync.WaitGroup{},
	}, nil
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
	ops confluence.Inlet[[]retrieveOperation]
	kve kv.KV
	ckv channelKV
}

type nilExecutor struct{ query.Executor }

func (r retrieveFactory) New() Retrieve {
	return Retrieve{Query: query.New(nilExecutor{}), ops: r.ops, kve: r.kve, ckv: r.ckv}
}

// |||||| OPERATION ||||||

type unaryRetrieveOperation struct {
	seg      SugaredSegment
	outlet   confluence.Inlet[RetrieveResponse]
	dataRead alamos.Duration
	wg       *sync.WaitGroup
	ctx      context.Context
}

// Context implements persist.Operation.
func (ro unaryRetrieveOperation) Context() context.Context { return ro.ctx }

// FileKey implements persist.Operation.
func (ro unaryRetrieveOperation) FileKey() fileKey { return ro.seg.fileKey }

// WriteError implements persist.Operation.
func (ro unaryRetrieveOperation) WriteError(err error) {
	ro.outlet.Inlet() <- RetrieveResponse{Err: err}
}

// Exec implements persist.Operation.
func (ro unaryRetrieveOperation) Exec(f file) {
	if ro.wg != nil {
		defer ro.wg.Done()
	}
	s := ro.dataRead.Stopwatch()
	s.Start()
	b := make([]byte, ro.seg.Size())
	_, err := f.ReadAt(b, ro.seg.Offset())
	if err == io.EOF {
		panic("retrieve operation: encountered unexpected EOF. this is a bug.")
	}
	ro.seg.Data = b
	ro.outlet.Inlet() <- RetrieveResponse{Segments: []Segment{ro.seg.Segment}, Err: err}
}

// Offset implements batch.RetrieveOperation.
func (ro unaryRetrieveOperation) Offset() int64 { return ro.seg.Offset() }

// ||||||| BATCH |||||||

type retrieveBatch struct {
	confluence.Transform[[]retrieveOperation]
}

func newRetrieveBatch() retrieveSegment {
	rb := &retrieveBatch{}
	rb.Transform.Transform = rb.batch
	return rb
}

func (r *retrieveBatch) batch(ctx confluence.Context, ops []retrieveOperation) ([]retrieveOperation, bool) {
	if len(ops) == 0 {
		return nil, false
	}
	files := make(map[fileKey][]retrieveOperation)
	for _, op := range ops {
		files[op.FileKey()] = append(files[op.FileKey()], op)
	}
	sets := make([]retrieveOperation, 0, len(files))
	for _, ops := range files {
		sort.Slice(ops, func(i, j int) bool { return ops[i].Offset() < ops[j].Offset() })
		sets = append(sets, retrieveOperationSet{Set: operation.Set[fileKey, retrieveOperation](ops)})
	}
	return sets, true
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
		kvRetrieve: alamos.NewGaugeDuration(sub, alamos.Debug, "kvRetrieveDur"),
		dataRead:   alamos.NewGaugeDuration(sub, alamos.Debug, "dataReadDur"),
		segSize:    alamos.NewGauge[int](sub, alamos.Debug, "segSize"),
		segCount:   alamos.NewGauge[int](sub, alamos.Debug, "segCount"),
		request:    alamos.NewGaugeDuration(sub, alamos.Debug, "requestDur"),
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
	pipeline := confluence.NewPipeline[[]retrieveOperation]()
	pipeline.Segment("queue", &queue.Debounce[retrieveOperation]{DebounceConfig: cfg.debounce})
	pipeline.Segment("batch", newRetrieveBatch())
	pipeline.Segment("persist", persist.New[fileKey, retrieveOperation](cfg.fs, cfg.persist))
	rb := pipeline.NewRouteBuilder()
	rb.Route(confluence.UnaryRouter[[]retrieveOperation]{FromAddr: "queue", ToAddr: "batch", Capacity: 10})
	rb.Route(confluence.UnaryRouter[[]retrieveOperation]{FromAddr: "batch", ToAddr: "persist", Capacity: 10})
	rb.RouteInletTo("queue")
	inlet := confluence.NewStream[[]retrieveOperation](10)
	pipeline.InFrom(inlet)
	return retrieveFactory{ops: inlet, kve: cfg.kv, ckv: channelKV{kv: cfg.kv}}, rb.Error()
}
