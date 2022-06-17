package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/telem"
	"sync"
)

type StreamIterator interface {
	// Source is the outlet for the StreamIterator values. All segments read from disk
	// are piped to the Source outlet. Iterate should be the ONLY entity writing to the
	// Source outlet (StreamIterator.Close will close the Source outlet).
	confluence.Source[RetrieveResponse]
	// Next pipes the next segment in the StreamIterator to the Source outlet.
	// It returns true if the StreamIterator is pointing to a valid segment.
	Next() bool
	// First seeks to the first segment in the StreamIterator. Returns true
	// if the streamIterator is pointing to a valid segment.
	First() bool
	// Last seeks to the last segment in the StreamIterator. Returns true
	// if the streamIterator is pointing to a valid segment.
	Last() bool
	// NextSpan pipes all segments in the StreamIterator from the current position to
	// the end of the span. It returns true if the streamIterator is pointing to a
	// valid segment. If span is TimeSpanMax, it will exhaust the streamIterator. If
	// span is TimeSpanZero, it won't do anything.
	NextSpan(span TimeSpan) bool
	// PrevSpan pipes all segments in the StreamIterator from the current
	PrevSpan(span TimeSpan) bool
	// NextRange seeks the StreamIterator to the start of the provided range and pipes all segments bound by it
	// to the Source outlet. It returns true if the streamIterator is pointing to a valid segment.
	// If range is TimeRangeMax, exhausts the StreamIterator. If range is TimeRangeZero, it won't do anything.
	NextRange(tr telem.TimeRange) bool
	// SeekLT seeks the StreamIterator to the first segment with a timestamp less than the provided timestamp.
	// It returns true if the StreamIterator is pointing to a valid segment.
	SeekLT(time TimeStamp) bool
	// SeekGE seeks the StreamIterator to the first segment with a timestamp greater than or equal to the provided timestamp.
	// It returns true if the StreamIterator is pointing to a valid segment.
	SeekGE(time TimeStamp) bool
	// View returns the current range of values the StreamIterator has a 'view' of.  This view represents the range of
	// segments most recently returned to the caller.
	View() TimeRange
	// Exhaust exhausts the StreamIterator, piping all segments to the Source outlet.
	// Cancelling the context results in an immediate abort of all ongoing operations.
	Exhaust(ctx context.Context)
	// Error returns the error encountered during the last call to the StreamIterator.
	// This value is reset after iteration.
	Error() error
	// Close closes the StreamIterator, ensuring that all in-progress segment reads complete before closing the Source outlet.
	Close() error
}

type streamIterator struct {
	// internal is the iterator that traverses segment metadata in key-value storage. It's essentially the 'brains'
	// behind the operations.
	internal *kv.Iterator
	// UnarySource is where values from the iterator will be piped.
	confluence.UnarySource[RetrieveResponse]
	// parser converts segment metadata into executable operations on disk.
	parser *retrieveParser
	// executor is an Outlet where generated operations are piped for execution.
	executor confluence.Inlet[[]retrieveOperation]
	// wg is used to track the completion status of the latest operations in the iterator.
	wg *sync.WaitGroup
	// _error represents a general error encountered during iterator. This value is reset whenever a seeking
	// call is made.
	_error error
}

// Next implements StreamIterator.
func (i *streamIterator) Next() bool {
	if !i.internal.Next() {
		return false
	}
	return true
}

// First implements StreamIterator.
func (i *streamIterator) First() bool {
	if !i.internal.First() {
		return false
	}
	i.pipeOperations()
	return true
}

// Last implements StreamIterator.
func (i *streamIterator) Last() bool {
	if !i.internal.Last() {
		return false
	}
	i.pipeOperations()
	return true
}

// NextSpan implements StreamIterator.
func (i *streamIterator) NextSpan(span TimeSpan) bool {
	if !i.internal.NextSpan(span) {
		return false
	}
	i.pipeOperations()
	return true
}

// PrevSpan implements StreamIterator.
func (i *streamIterator) PrevSpan(span TimeSpan) bool {
	if !i.internal.PrevSpan(span) {
		return false
	}
	i.pipeOperations()
	return true
}

// NextRange implements StreamIterator.
func (i *streamIterator) NextRange(tr TimeRange) bool {
	if !i.internal.NextRange(tr) {
		return false
	}
	i.pipeOperations()
	return true
}

// SeekLT implements StreamIterator.
func (i *streamIterator) SeekLT(stamp TimeStamp) bool { return i.internal.SeekLT(stamp) }

// SeekGE implements StreamIterator.
func (i *streamIterator) SeekGE(stamp TimeStamp) bool { return i.internal.SeekGE(stamp) }

// Seek implements StreamIterator.
func (i *streamIterator) Seek(stamp TimeStamp) bool { return i.internal.Seek(stamp) }

// View implements StreamIterator.
func (i *streamIterator) View() TimeRange { return i.internal.View() }

// Error implements StreamIterator.
func (i *streamIterator) Error() error {
	if i.error() != nil {
		return i.error()
	}
	return i.internal.Error()
}

// Exhaust implements StreamIterator.
func (i *streamIterator) Exhaust(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			i.Out.Inlet() <- RetrieveResponse{Error: ctx.Err()}
			break
		}
		if !i.Next() {
			break
		}
		if i.Error() != nil {
			i.Out.Inlet() <- RetrieveResponse{Error: i.Error()}
			break
		}
		i.pipeOperations()
	}
}

// Close implements StreamIterator.
func (i *streamIterator) Close() error {
	i.updateError(i.internal.Close())
	i.wg.Wait()
	close(i.Out.Inlet())
	return i.error()
}

func (i *streamIterator) pipeOperations() {
	ops, err := i.parser.Parse(i.UnarySource, i.wg, i.internal.Value())
	i.updateError(err)
	if len(ops) == 0 {
		return
	}
	i.executor.Inlet() <- ops
}

func (i *streamIterator) updateError(err error) {
	if err != nil {
		i._error = err
	}
}

func (i *streamIterator) error() error {
	if i._error != nil {
		return i._error
	}
	return i.internal.Error()
}
