package cesium

import (
	"cesium/internal/errutil"
	"cesium/internal/kv"
	"cesium/internal/operation"
	"cesium/internal/wg"
	"cesium/shut"
	"context"
	"go.uber.org/zap"
	"go/types"
	"io"
	"sort"
)

type (
	retrieveStream       = stream[types.Nil, RetrieveResponse]
	retrieveWaitGroup    = wg.Slice[retrieveOperation]
	retrieveOperationSet = operation.Set[fileKey, retrieveOperation]
)

// ||||||| OPERATION ||||||

type retrieveOperation struct {
	seg    Segment
	stream *retrieveStream
	done   chan struct{}
	ctx    context.Context
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
	ro.stream.res <- RetrieveResponse{Err: err}
}

// Exec implements persist.Operation.
func (ro retrieveOperation) Exec(f file) {
	c := errutil.NewCatchReadWriteSeek(f)
	c.Seek(ro.seg.offset, io.SeekStart)
	b := make([]byte, ro.seg.size)
	c.Read(b)
	ro.seg.Data = b
	if c.Error() == io.EOF {
		panic("get encountered unexpected EOF. this is a bug.")
	}
	ro.stream.res <- RetrieveResponse{Segments: []Segment{ro.seg}, Err: c.Error()}
	ro.done <- struct{}{}
}

// Offset implements batch.RetrieveOperation.
func (ro retrieveOperation) Offset() int64 {
	return ro.seg.offset
}

// |||||| PARSER ||||||

type retrieveParser struct {
	ckv    channelKV
	skv    segmentKV
	logger *zap.Logger
}

func (rp retrieveParser) parse(ctx context.Context, q query) (w retrieveWaitGroup, err error) {
	keys, err := channelKeys(q, true)
	if err != nil {
		rp.logger.Error("failed to parse query", zap.Error(err))
		return w, err
	}
	// Check if the channels exist.
	if _, err := rp.ckv.getMultiple(keys...); err != nil {
		rp.logger.Error("failed to get channels", zap.Error(err))
		return w, err
	}
	tr := timeRange(q)
	from, to := generateRangeKeys(keys[0], tr)
	rp.logger.Debug("retrieving segments",
		zap.Int("count", len(keys)),
		zap.Time("from", tr.Start.Time()),
		zap.Time("to", tr.End.Time()),
		zap.Binary("from-key", from),
		zap.Binary("to-key", to),
		zap.Binary("prefix", kv.CompositeKey(segmentKVPrefix, keys[0])),
	)
	var segments []Segment
	for _, key := range keys {
		nSegments, err := rp.skv.filter(tr, key)
		if err != nil {
			return w, err
		}
		if len(nSegments) == 0 {
			return w, newSimpleError(ErrNotFound, "no data found to satisfy query")
		}
		segments = append(segments, nSegments...)
	}
	w.Done = make(chan struct{}, len(segments))
	s := getStream[types.Nil, RetrieveResponse](q)
	for _, seg := range segments {
		w.Items = append(w.Items, retrieveOperation{seg: seg, stream: s, done: w.Done, ctx: ctx})
	}
	return w, nil
}

// |||||| EXECUTOR ||||||

type retrieveQueryExecutor struct {
	parser   retrieveParser
	queue    chan<- []retrieveOperation
	shutdown shut.Shutdown
	logger   *zap.Logger
}

// exec implements queryExecutor.
func (rp retrieveQueryExecutor) exec(ctx context.Context, q query) error {
	w, err := rp.parser.parse(ctx, q)
	if err != nil {
		return err
	}
	s := getStream[types.Nil, RetrieveResponse](q)
	rp.shutdown.Go(func(_ chan shut.Signal) error {
		// We don't care about the signal. The query will finish when it finishes.
		w.Wait()
		close(s.res)
		return nil
	})
	rp.queue <- w.Items
	return nil
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
