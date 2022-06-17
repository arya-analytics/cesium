package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/core"
	ckv "github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/query"
	"github.com/arya-analytics/x/queue"
	"github.com/arya-analytics/x/shutdown"
	"go.uber.org/zap"
	"sync"
	"time"
)

type (
	retrieveSegment = confluence.Segment[[]retrieveOperation]
)

// |||||| STREAM ||||||

// RetrieveResponse is a response containing segments satisfying a Retrieve Query as well as any errors
// encountered during the retrieval.
type RetrieveResponse struct {
	Error    error
	Segments []segment.Segment
}

// |||||| QUERY ||||||

type Retrieve struct {
	query.Query
	kve    kv.KV
	ops    confluence.Inlet[[]retrieveOperation]
	parser *retrieveParser
}

// WhereChannels sets the channels to retrieve data for.
// If no keys are provided, will return an ErrInvalidQuery error.
func (r Retrieve) WhereChannels(keys ...ChannelKey) Retrieve { channel.SetKeys(r, keys...); return r }

// WhereTimeRange sets the time range to retrieve data from.
func (r Retrieve) WhereTimeRange(tr TimeRange) Retrieve { setTimeRange(r, tr); return r }

// Stream streams all segments from the iterator out to the channel. Errors encountered
// during stream construction are returned immediately. Errors encountered during
// segment reads are returns as part of RetrieveResponse.
func (r Retrieve) Stream(ctx context.Context) (<-chan RetrieveResponse, error) {
	stream := confluence.NewStream[RetrieveResponse](10)
	iter, err := r.Iterate()
	if err != nil {
		return nil, err
	}
	iter.OutTo(stream)
	iter.First()
	go func() {
		iter.Exhaust(ctx)
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()
	return stream.Outlet(), nil
}

func (r Retrieve) Iterate() (StreamIterator, error) {
	iter := &streamIterator{
		internal: ckv.NewIterator(r.kve, timeRange(r), channel.GetKeys(r)...),
		executor: r.ops,
		parser:   r.parser,
		wg:       &sync.WaitGroup{},
	}
	return iter, iter.error()
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
	ops    confluence.Inlet[[]retrieveOperation]
	kve    kv.KV
	parser *retrieveParser
}

func (r retrieveFactory) New() Retrieve {
	return Retrieve{
		Query:  query.New(),
		ops:    r.ops,
		kve:    r.kve,
		parser: r.parser,
	}
}

// ||||||| BATCH |||||||

// |||||| METRICS ||||||

type retrieveMetrics struct {
	// kvRetrieve is the time spent retrieving segment metadata from key-value storage.
	kvRetrieve alamos.Duration
	// dataRead is the duration spent reading segment data from disk.
	dataRead alamos.Duration
	// segSize tracks the Size of each segment retrieved.
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
	// fs is the file system for reading segment data from.
	fs core.FS
	// kv is the key-value store for reading segment metadata from.
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
	retrieveDebounceFlushInterval = 10 * time.Millisecond
	// retrieveDebounceFlushThreshold is the number of retrieve operations that must be in the debounce queue before
	// it flushes
	retrieveDebounceFlushThreshold = 10
)

type retrieveParser struct {
	logger  *zap.Logger
	metrics retrieveMetrics
}

func (r *retrieveParser) Parse(
	source confluence.UnarySource[RetrieveResponse],
	wg *sync.WaitGroup,
	ranges []*segment.Range,
) ([]retrieveOperation, error) {
	var ops []retrieveOperation
	for _, rng := range ranges {
		wg.Add(len(rng.Headers))
		for _, header := range rng.Headers {
			seg := header.Sugar(rng.Channel)
			seg.SetBounds(rng.Bound)
			ops = append(ops, retrieveOperationUnary{
				seg:         seg,
				ctx:         context.Background(),
				dataRead:    r.metrics.dataRead,
				wg:          wg,
				logger:      r.logger,
				UnarySource: source,
			})
		}
	}
	return ops, nil
}

func startRetrieve(cfg retrieveConfig) (query.Factory[Retrieve], error) {
	mergeRetrieveConfigDefaults(&cfg)
	pipeline := confluence.NewPipeline[[]retrieveOperation]()
	pipeline.Segment("queue", &queue.Debounce[retrieveOperation]{DebounceConfig: cfg.debounce})
	pipeline.Segment("batch", newRetrieveBatch())
	pipeline.Segment("persist", persist.New[core.FileKey, retrieveOperation](cfg.fs, cfg.persist))
	rb := pipeline.NewRouteBuilder()
	rb.Route(confluence.UnaryRouter[[]retrieveOperation]{FromAddr: "queue", ToAddr: "batch", Capacity: 10})
	rb.Route(confluence.UnaryRouter[[]retrieveOperation]{FromAddr: "batch", ToAddr: "persist", Capacity: 10})
	rb.RouteInletTo("queue")
	inlet := confluence.NewStream[[]retrieveOperation](10)
	pipeline.InFrom(inlet)
	ctx := confluence.DefaultContext()
	ctx.Shutdown = cfg.shutdown
	pipeline.Flow(ctx)
	return retrieveFactory{
		ops:    inlet,
		kve:    cfg.kv,
		parser: &retrieveParser{metrics: newRetrieveMetrics(cfg.exp), logger: cfg.logger},
	}, rb.Error()
}
