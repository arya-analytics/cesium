package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/allocate"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/core"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/telem"
	"go.uber.org/zap"
	"io"
	"sync"
)

// |||||| RETRIEVE ||||||

type retrieveOperation interface {
	operation.Operation[core.FileKey]
	Offset() telem.Offset
}

// retrieveOperationUnary represents a single segment read.
type retrieveOperationUnary struct {
	ctx      context.Context
	seg      *segment.Sugared
	outlet   confluence.Inlet[RetrieveResponse]
	dataRead alamos.Duration
	wg       *sync.WaitGroup
}

// Context implements retrieveOperation.
func (ro retrieveOperationUnary) Context() context.Context { return ro.ctx }

// FileKey implements retrieveOperation.
func (ro retrieveOperationUnary) FileKey() core.FileKey { return ro.seg.FileKey() }

// WriteError implements retrieveOperation.
func (ro retrieveOperationUnary) WriteError(err error) {
	ro.outlet.Inlet() <- RetrieveResponse{Error: err}
}

// Offset implements retrieveOperation.
func (ro retrieveOperationUnary) Offset() telem.Offset { return ro.seg.BoundedOffset() }

// Exec implements persist.Operation.
func (ro retrieveOperationUnary) Exec(f core.File) {
	if ro.wg != nil {
		defer ro.wg.Done()
	}
	s := ro.dataRead.Stopwatch()
	s.Start()
	err := ro.seg.ReadDataFrom(f)
	if err == io.EOF {
		panic("[cesium] unexpected EOF encountered while reading segment")
	}
	s.Stop()
	ro.outlet.Inlet() <- RetrieveResponse{Segments: []segment.Segment{ro.seg.Segment()}, Error: err}
}

// retrieveOperationSet represents a set of retrieveOperations to execute together.
// The operations in the set are assumed to be ordered by file offset. All operations
// should have the same file key.
type retrieveOperationSet struct {
	operation.Set[core.FileKey, retrieveOperation]
}

// Offset implements retrieveOperation.
func (s retrieveOperationSet) Offset() telem.Offset { return s.Set[0].Offset() }

// |||||| CREATE ||||||

type createOperation interface {
	operation.Operation[core.FileKey]
	allocate.Item[channel.Key]
	ChannelKey() channel.Key
}

type unaryCreateOperation struct {
	seg     *segment.Sugared
	ctx     context.Context
	logger  *zap.Logger
	metrics createMetrics
	wg      *sync.WaitGroup
	kv      *kv.Header
	confluence.UnarySource[CreateResponse]
}

// Context implements createOperation.
func (cr unaryCreateOperation) Context() context.Context { return cr.ctx }

// FileKey implements createOperation.
func (cr unaryCreateOperation) FileKey() core.FileKey { return cr.seg.FileKey() }

// ChannelKey implements createOperation.
func (cr unaryCreateOperation) ChannelKey() channel.Key { return cr.seg.ChannelKey() }

// WriteError implements createOperation.
func (cr unaryCreateOperation) WriteError(err error) { cr.Out.Inlet() <- CreateResponse{Error: err} }

// BindWaitGroup implements createOperation.
func (cr unaryCreateOperation) BindWaitGroup(wg *sync.WaitGroup) { cr.wg = wg }

// Size implements allocate.Item.
func (cr unaryCreateOperation) Size() telem.Size { return cr.seg.UnboundedSize() }

// Key implements allocate.Item.
func (cr unaryCreateOperation) Key() channel.Key { return cr.ChannelKey() }

// Exec implements persist.Operation.
func (cr unaryCreateOperation) Exec(f core.File) {
	if cr.wg != nil {
		defer cr.wg.Done()
	}
	totalFlush := cr.metrics.totalFlush.Stopwatch()
	totalFlush.Start()
	defer totalFlush.Stop()

	if err := cr.seg.WriteDataTo(f); err != nil {
		cr.WriteError(err)
		return
	}
	cr.metrics.dataFlush.Record(totalFlush.Elapsed())

	ks := cr.metrics.kvFlush.Stopwatch()
	ks.Start()
	if err := cr.seg.WriteMetaDataTo(cr.kv); err != nil {
		cr.WriteError(err)
		return
	}
	ks.Stop()
}
