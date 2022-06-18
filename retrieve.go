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

type retrieveSegment = confluence.Segment[[]retrieveOperation]

// |||||| CONFIGURATION ||||||

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
	kve     kv.KV
	ops     confluence.Inlet[[]retrieveOperation]
	metrics retrieveMetrics
	logger  *zap.Logger
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
	iter := r.Iterate(ctx)
	if err := iter.Error(); err != nil {
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

func (r Retrieve) Iterate(ctx context.Context) StreamIterator {
	responses := &confluence.UnarySource[RetrieveResponse]{}
	wg := &sync.WaitGroup{}
	return &streamIterator{
		internal: ckv.NewIterator(r.kve, timeRange(r), channel.GetKeys(r)...),
		executor: r.ops,
		parser: &retrieveParser{
			ctx:       ctx,
			logger:    r.logger,
			responses: responses,
			wg:        wg,
			metrics:   r.metrics,
		},
		wg:          wg,
		UnarySource: responses,
	}
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
	ops     confluence.Inlet[[]retrieveOperation]
	kve     kv.KV
	logger  *zap.Logger
	metrics retrieveMetrics
}

func (r retrieveFactory) New() Retrieve {
	return Retrieve{
		Query:   query.New(),
		ops:     r.ops,
		kve:     r.kve,
		logger:  r.logger,
		metrics: r.metrics,
	}
}

func startRetrieve(cfg retrieveConfig) (query.Factory[Retrieve], error) {
	mergeRetrieveConfigDefaults(&cfg)

	pipeline := confluence.NewPipeline[[]retrieveOperation]()

	// queue 'debounces' operations so that they can be flushed to disk in efficient
	// batches.
	pipeline.Segment("queue", &queue.Debounce[retrieveOperation]{DebounceConfig: cfg.debounce})

	// batch groups operations into batches that optimize sequential IO.
	pipeline.Segment("batch", newRetrieveBatch())

	// persist executeds batched operations on disk.
	pipeline.Segment("persist", persist.New[core.FileKey, retrieveOperation](cfg.fs, cfg.persist))

	rb := pipeline.NewRouteBuilder()

	rb.Route(confluence.UnaryRouter[[]retrieveOperation]{
		FromAddr: "queue",
		ToAddr:   "batch",
		Capacity: 10,
	})

	rb.Route(confluence.UnaryRouter[[]retrieveOperation]{
		FromAddr: "batch",
		ToAddr:   "persist",
		Capacity: 10,
	})

	rb.RouteInletTo("queue")

	// inlet is the inlet for all retrieve operations to be executed on disk.
	inlet := confluence.NewStream[[]retrieveOperation](1)

	pipeline.InFrom(inlet)

	ctx := confluence.DefaultContext()
	ctx.Shutdown = cfg.shutdown

	pipeline.Flow(ctx)

	return retrieveFactory{
		ops:     inlet,
		kve:     cfg.kv,
		metrics: newRetrieveMetrics(cfg.exp),
		logger:  cfg.logger,
	}, rb.Error()
}
