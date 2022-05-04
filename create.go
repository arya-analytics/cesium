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
	"github.com/arya-analytics/cesium/internal/queue"
	"github.com/arya-analytics/cesium/shut"
	"go.uber.org/zap"
	"io"
	"sync"
	"time"
)

type (
	createStream       = stream[CreateRequest, CreateResponse]
	segmentAllocator   = allocate.Allocator[ChannelKey, fileKey, Segment]
	createOperationSet = operation.Set[fileKey, createOperation]
)

// |||||| OPERATION ||||||

type createOperation struct {
	fileKey    fileKey
	channelKey ChannelKey
	segments   []Segment
	segmentKV  segmentKV
	stream     *createStream
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
	cr.stream.res <- CreateResponse{Err: err}
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
	stream    *createStream
	logger    *zap.Logger
	metrics   createMetrics
}

func (cp *createParser) parse(ctx context.Context, req CreateRequest, wg *sync.WaitGroup) (ops []createOperation, err error) {
	cp.logger.Debug("parsing create requests", zap.Int("nSegments", len(req.Segments)))

	fileKeys := cp.allocator.Allocate(req.Segments...)

	wg.Add(len(req.Segments))

	for i, seg := range req.Segments {
		if seg.ChannelKey == 0 {
			return ops, newSimpleError(ErrInvalidQuery, "segment must be assigned a channel key")
		}
		if seg.Start.IsZero() {
			return ops, newSimpleError(ErrInvalidQuery, "segment must have a start time")
		}
		cp.metrics.dataSize.Record(seg.Size())
		ops = append(ops, createOperation{
			fileKey:    fileKeys[i],
			channelKey: seg.ChannelKey,
			segments:   []Segment{seg},
			segmentKV:  cp.skv,
			stream:     cp.stream,
			ctx:        ctx,
			logger:     cp.logger,
			metrics:    cp.metrics,
			wg:         wg,
		})
	}
	cp.logger.Debug("generated create operations", zap.Int("nOperations", len(ops)))
	return ops
}

// |||||| EXECUTOR ||||||

type createQueryExecutor struct {
	allocator segmentAllocator
	skv       segmentKV
	ckv       channelKV
	queue     chan<- []createOperation
	lock      lock.Map[ChannelKey]
	shutdown  shut.Shutdown
	logger    *zap.Logger
	metrics   createMetrics
}

// exec implements queryExecutor.
func (cp *createQueryExecutor) exec(ctx context.Context, q query) error {
	rt := cp.metrics.request.Stopwatch()
	rt.Start()
	keys, err := channelKeys(q, true)
	if err != nil {
		return err
	}
	lat := cp.metrics.lockAcquire.Stopwatch()
	lat.Start()
	if err := cp.lock.Acquire(keys...); err != nil {
		return err
	}
	lat.Stop()
	s := getStream[CreateRequest, CreateResponse](q)
	parse := &createParser{
		skv:       cp.skv,
		allocator: cp.allocator,
		stream:    s,
		logger:    cp.logger,
		metrics:   cp.metrics,
	}
	cp.shutdown.Go(func(sig chan shut.Signal) error {
		wg := sync.WaitGroup{}
	o:
		for {
			select {
			case <-sig:
				break o
			case <-ctx.Done():
				return nil
			case req, ok := <-s.req:
				if !ok {
					break o
				}
				cp.queue <- parse.parse(ctx, req, &wg)
			}
		}
		wg.Wait()
		lrt := cp.metrics.lockRelease.Stopwatch()
		lrt.Start()
		cp.lock.Release(keys...)
		lrt.Stop()
		rt.Stop()
		close(s.res)
		return nil
	})
	return nil
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
	dataSize    alamos.Metric[int]
	request     alamos.Duration
}

// |||||| START UP |||||||

func startCreatePipeline(fs fileSystem, kve kv.KV, opts *options, sd shut.Shutdown) (queryExecutor, error) {

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

	counter, err := kv.NewPersistedCounter(kve, []byte("cesium-nextFile"))
	if err != nil {
		return nil, err
	}
	nextFile := &fileCounter{PersistedCounter: *counter}

	alloc := allocate.New[ChannelKey, fileKey, Segment](nextFile, allocate.Config{
		MaxDescriptors: 10,
		MaxSize:        1e9,
	})

	exp := alamos.Sub(opts.exp, "create")
	return &createQueryExecutor{
		allocator: alloc,
		skv:       segmentKV{kv: kve},
		ckv:       channelKV{kv: kve},
		queue:     q.In,
		lock:      lock.NewMap[ChannelKey](),
		shutdown:  sd,
		logger:    opts.logger,
		metrics: createMetrics{
			dataSize:    alamos.NewGauge[int](exp, "dataSize"),
			lockAcquire: alamos.NewGaugeDuration(exp, "lockAcquireTime"),
			lockRelease: alamos.NewGaugeDuration(exp, "lockReleaseTime"),
			dataFlush:   alamos.NewGaugeDuration(exp, "dataFlushTime"),
			kvFlush:     alamos.NewGaugeDuration(exp, "kvFlushTime"),
			totalFlush:  alamos.NewGaugeDuration(exp, "totalFlushTime"),
			request:     alamos.NewGaugeDuration(exp, "requestTime"),
		},
	}, nil
}

type queryPackage[Q Query] struct {
	Assembler func(qExec queryExecutor) Q
	Validators
}
