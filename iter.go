package cesium

import (
	"context"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/telem"
	"github.com/cockroachdb/pebble"
	"io"
)

type Iterator interface {
	Next() bool
	NextSpan(span TimeSpan) bool
	NextRange(tr telem.TimeRange) bool
	First() bool
	Last() bool
	SeekLT(time TimeStamp) bool
	SeekGE(time TimeStamp) bool
	confluence.Source[RetrieveResponse]
}

type segmentKVIterator struct {
	// Iterator is the underlying key-value iterator.
	kv.Iterator
	// pos tracks the current position of the iterator.
	pos TimeStamp
	// channel  is the channel the iterator is iterating over.
	channel Channel
	// segments are the current segments in the iterator.
	segments []sugaredSegment
	// bounds represents the bounds for the current iterator value.
	bounds TimeRange
	// err is the error that occurred during iteration.
	err error
}

type sugaredSegment struct {
	Segment
	Bound   TimeRange
	Channel Channel
}

func (b sugaredSegment) makeData() []byte { return make([]byte, b.endOffset()-b.startOffset()) }

func (b sugaredSegment) startOffset() int64 {
	boundOffset := TimeSpan(b.boundedRange().Start-b.Start).ByteCount(b.Channel.DataRate, b.Channel.DataType)
	return b.Segment.offset + boundOffset
}

func (b sugaredSegment) endOffset() int64 {
	boundOffset := TimeSpan(b.boundedRange().End-b.Start).ByteCount(b.Channel.DataRate, b.Channel.DataType)
	return b.Segment.offset + boundOffset
}

func (b sugaredSegment) boundedRange() TimeRange { return b.baseRange().Bound(b.Bound) }

func (b sugaredSegment) baseRange() TimeRange { return b.Start.SpanRange(b.span()) }

func (b sugaredSegment) span() TimeSpan {
	return b.Channel.DataRate.ByteSpan(b.Segment.Size(), b.Channel.DataType)
}

func newSegmentKVIterator(channel Channel, rng telem.TimeRange, kve kv.KV) *segmentKVIterator {
	sg := &segmentKVIterator{pos: rng.Start, channel: channel}
	kvIter := kve.IterRange(sg.rangeKey(rng.Start), sg.rangeKey(rng.End))
	sg.Iterator = kvIter
	return &segmentKVIterator{}
}

func (si *segmentKVIterator) rangeKey(stamp TimeStamp) []byte {
	return Segment{Start: stamp, ChannelKey: si.channel.Key}.KVKey()
}

func (si *segmentKVIterator) Next() bool {
	si.gcValue()
	if !si.Iterator.Next() {
		return false
	}
	si.loadValue()
	si.bounds.Start = si.pos
	seg := si.segments[0]
	si.pos = seg.Start.Add(si.channel.DataRate.ByteSpan(seg.Size(), si.channel.DataType))
	return true
}

func (si *segmentKVIterator) NextSpan(span TimeSpan) bool {
	si.gcValue()
	endTS := si.pos.Add(span)
	limit := si.rangeKey(endTS)
	for {
		if state := si.Iterator.NextWithLimit(limit); state != pebble.IterValid {
			break
		}
		si.loadValue()
	}
	si.bounds = TimeRange{Start: si.pos, End: endTS}
	si.pos = endTS
	return si.segmentsValid()
}

func (si *segmentKVIterator) NextRange(tr telem.TimeRange) bool {
	si.gcValue()
	if !si.SeekLT(tr.Start) {
		return false
	}
	return si.NextSpan(tr.Span())
}

func (si *segmentKVIterator) SeekLT(stamp TimeStamp) bool {
	si.gcValue()
	si.pos = stamp
	return si.Iterator.SeekLT(si.rangeKey(stamp))
}

func (si *segmentKVIterator) SeekGE(stamp TimeStamp) bool {
	si.gcValue()
	si.pos = stamp
	return si.Iterator.SeekGE(si.rangeKey(stamp))
}

func (si *segmentKVIterator) Value() []sugaredSegment {
	if !si.segmentsValid() {
		return nil
	}
	last, first := si.segments[0], si.segments[len(si.segments)-1]
	first.Bound.Start = si.bounds.Start
	last.Bound.End = si.bounds.End
	return si.segments
}

func (si *segmentKVIterator) gcValue() {
	si.segments = []sugaredSegment{}
	si.bounds = TimeRangeMax
}

func (si *segmentKVIterator) segmentsValid() bool { return len(si.segments) > 0 }

func (si *segmentKVIterator) loadValue() {
	seg := Segment{}
	if err := kv.LoadBytes(si.Iterator.Value(), &seg); err != nil {
		si.err = err
	}
	si.segments = append(si.segments, sugaredSegment{Segment: seg, Channel: si.channel, Bound: TimeRangeMax})
}

type iterator struct {
	kvIter segmentKVIterator
	confluence.UnarySource[RetrieveResponse]
	ops confluence.Inlet[[]iteratorRetrieveOperation]
}

func (i *iterator) Next() bool {
	if !i.kvIter.Next() {
		return false
	}
	i.ops.Inlet() <- i.opGen(i.kvIter.Value())
	return true
}

func (i *iterator) opGen(segments []sugaredSegment) (ops []iteratorRetrieveOperation) {
	for _, seg := range segments {
		ops = append(ops, iteratorRetrieveOperation{seg: seg, outlet: i.Out})
	}
	return ops
}

type iteratorRetrieveOperation struct {
	seg    sugaredSegment
	outlet confluence.Inlet[RetrieveResponse]
}

// Context implements persist.Operation.
func (ro iteratorRetrieveOperation) Context() context.Context { return context.Background() }

// FileKey implements persist.Operation.
func (ro iteratorRetrieveOperation) FileKey() fileKey { return ro.seg.fileKey }

// WriteError implements persist.Operation.
func (ro iteratorRetrieveOperation) WriteError(err error) {
	ro.outlet.Inlet() <- RetrieveResponse{Err: err}
}

// Exec implements persist.Operation.
func (ro iteratorRetrieveOperation) Exec(f file) {
	b := ro.seg.makeData()
	_, err := f.ReadAt(b, ro.seg.startOffset())
	if err == io.EOF {
		panic("retrieve operation: encountered unexpected EOF. this is a bug.")
	}
	ro.seg.Data = b
	ro.outlet.Inlet() <- RetrieveResponse{Segments: []Segment{ro.seg.Segment}, Err: err}
}

// Offset implements batch.RetrieveOperation.
func (ro iteratorRetrieveOperation) Offset() int64 { return ro.seg.startOffset() }
