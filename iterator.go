package cesium

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/telem"
	"sync"
)

type StreamIterator interface {
	Iterator
	// Source is the outlet for the StreamIterator values. All segments read from disk are piped
	// to the Source outlet. Iterator should be the ONLY entity writing to the Source outlet
	// (Iterator.Close will close the Source outlet).
	confluence.Source[RetrieveResponse]
}

type Iterator interface {
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
	// Position returns the current StreamIterator position.
	Position() TimeStamp
	// Exhaust exhausts the StreamIterator, piping all segments to the Source outlet.
	Exhaust()
	// Error returns the error encountered during the last call to the StreamIterator.
	// This value is reset after iteration.
	Error() error
	// Close closes the StreamIterator, ensuring that all in-progress Segment reads complete before closing the Source outlet.
	Close() error
}

type streamIterator struct {
	kvIterator
	confluence.UnarySource[RetrieveResponse]
	ops confluence.Inlet[[]retrieveOperation]
	wg  *sync.WaitGroup
}

// Next implements StreamIterator.
func (i *streamIterator) Next() bool {
	if !i.kvIterator.Next() {
		return false
	}
	i.ops.Inlet() <- i.operations(i.Value())
	return true
}

// First implements StreamIterator.
func (i *streamIterator) First() bool {
	if !i.kvIterator.First() {
		return false
	}
	i.pipeOperations()
	return true
}

// Last implements StreamIterator.
func (i *streamIterator) Last() bool {
	if !i.kvIterator.Last() {
		return false
	}
	i.pipeOperations()
	return true
}

// NextSpan implements StreamIterator.
func (i *streamIterator) NextSpan(span TimeSpan) bool {
	if !i.kvIterator.NextSpan(span) {
		return false
	}
	i.pipeOperations()
	return true
}

// NextRange implements StreamIterator.
func (i *streamIterator) NextRange(tr telem.TimeRange) bool {
	if !i.kvIterator.NextRange(tr) {
		return false
	}
	i.pipeOperations()
	return true
}

// Exhaust implements StreamIterator.
func (i *streamIterator) Exhaust() { i.NextSpan(TimeSpanMax); i.pipeOperations() }

// Close implements StreamIterator.
func (i *streamIterator) Close() error {
	err := i.kvIterator.Close()
	i.wg.Wait()
	close(i.Out.Inlet())
	return err
}

func (i *streamIterator) pipeOperations() { i.ops.Inlet() <- i.operations(i.Value()) }

func (i *streamIterator) operations(segments []SugaredSegment) []retrieveOperation {
	ops := make([]retrieveOperation, len(segments))
	i.wg.Add(len(segments))
	for _, seg := range segments {
		ops = append(ops, unaryRetrieveOperation{seg: seg, outlet: i.Out, wg: i.wg})
	}
	return ops
}
