package cesium

import (
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/telem"
)

type Iterator interface {
	Next() bool
	First() bool
	Last() bool
	NextSpan(span TimeSpan) bool
	NextRange(tr telem.TimeRange) bool
	SeekLT(time TimeStamp) bool
	SeekGE(time TimeStamp) bool
	Position() TimeStamp
	Exhaust()
	Error()
	confluence.Source[RetrieveResponse]
}

type iterator struct {
	kvIterator
	confluence.UnarySource[RetrieveResponse]
	ops confluence.Inlet[[]retrieveOperation]
}

func (i *iterator) Next() bool {
	if !i.kvIterator.Next() {
		return false
	}
	i.ops.Inlet() <- i.operations(i.Value())
	return true
}

func (i *iterator) First() bool {
	if !i.kvIterator.First() {
		return false
	}
	i.pipeOperations()
	return true
}

func (i *iterator) Last() bool {
	if !i.kvIterator.Last() {
		return false
	}
	i.pipeOperations()
	return true
}

func (i *iterator) NextSpan(span TimeSpan) bool {
	if !i.kvIterator.NextSpan(span) {
		return false
	}
	i.pipeOperations()
	return true
}

func (i *iterator) NextRange(tr telem.TimeRange) bool {
	if !i.kvIterator.NextRange(tr) {
		return false
	}
	i.pipeOperations()
	return true
}

func (i *iterator) Exhaust() {
	i.NextSpan(TimeSpanMax)
	i.pipeOperations()
}

func (i *iterator) pipeOperations() { i.ops.Inlet() <- i.operations(i.Value()) }

func (i *iterator) operations(segments []SugaredSegment) []retrieveOperation {
	ops := make([]retrieveOperation, len(segments))
	for _, seg := range segments {
		ops = append(ops, retrieveOperation{seg: seg, outlet: i.Out})
	}
	return ops
}
