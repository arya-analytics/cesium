package cesium

import (
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/telem"
	"sync"
)

type StreamIterator interface {
	// Source is the outlet for the StreamIterator values. All segments read from disk are piped
	// to the Source outlet. Iterator should be the ONLY entity writing to the Source outlet
	// (Iterator.Close will close the Source outlet).
	confluence.Source[RetrieveResponse]
	// Next pipes the next Segment in the StreamIterator to the Source outlet.
	// It returns true if the StreamIterator is pointing to a valid Segment.
	Next() bool
	// First seeks to the first Segment in the StreamIterator. Returns true
	// if the streamIterator is pointing to a valid Segment.
	First() bool
	// Last seeks to the last Segment in the StreamIterator. Returns true
	// if the streamIterator is pointing to a valid Segment.
	Last() bool
	// NextSpan pipes all segments in the StreamIterator from the current position to the end of the span.
	// It returns true if the streamIterator is pointing to a valid Segment. If span is TimeSpanMax, it will exhaust
	// the streamIterator. If span is TimeSpanZero, it won't do anything.
	NextSpan(span TimeSpan) bool
	// PrevSpan pipes all segments in the StreamIterator from the current
	PrevSpan(span TimeSpan) bool
	// NextRange seeks the StreamIterator to the start of the provided range and pipes all segments bound by it
	// to the Source outlet. It returns true if the streamIterator is pointing to a valid Segment.
	// If range is TimeRangeMax, exhausts the StreamIterator. If range is TimeRangeZero, it won't do anything.
	NextRange(tr telem.TimeRange) bool
	// SeekLT seeks the StreamIterator to the first Segment with a timestamp less than the provided timestamp.
	// It returns true if the StreamIterator is pointing to a valid Segment.
	SeekLT(time TimeStamp) bool
	// SeekGE seeks the StreamIterator to the first Segment with a timestamp greater than or equal to the provided timestamp.
	// It returns true if the StreamIterator is pointing to a valid Segment.
	SeekGE(time TimeStamp) bool
	// View returns the current range of values the StreamIterator has a 'view' of.  This view represents the range of
	// segments most recently returned to the caller.
	View() TimeStamp
	// Exhaust exhausts the StreamIterator, piping all segments to the Source outlet.
	Exhaust()
	// Error returns the error encountered during the last call to the StreamIterator.
	// This value is reset after iteration.
	Error() error
	// Close closes the StreamIterator, ensuring that all in-progress Segment reads complete before closing the Source outlet.
	Close() error
}

type streamIterator struct {
	// internal is the iterator that traverses segment metadata in key-value storage. It's essentially the 'brains'
	// behind the operations.
	internal *kv.Iterator
	// source is where values from the iterator will be piped.
	source confluence.UnarySource[RetrieveResponse]
	// parser converts segment metadata into executable operations on disk.
	parser operation.Parser[fileKey, retrieveOperation]
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

// NextRange implements StreamIterator.
func (i *streamIterator) NextRange(tr telem.TimeRange) bool {
	if !i.internal.NextRange(tr) {
		return false
	}
	i.pipeOperations()
	return true
}

// Exhaust implements StreamIterator.
func (i *streamIterator) Exhaust() {
	for {
		if !i.Next() {
			i.pipeOperations()
			break
		}
	}
}

// Close implements StreamIterator.
func (i *streamIterator) Close() error {
	i.updateError(i.internal.Close())
	i.wg.Wait()
	return i.error()
}

func (i *streamIterator) pipeOperations() {
	ops, err := i.parser.Parse(i.internal.Value())
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
