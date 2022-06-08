package cesium

import (
	"bytes"
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/telem"
	"github.com/cockroachdb/pebble"
	"io"
	"sort"
)

// |||||| CORE |||||||

type Segment struct {
	ChannelKey ChannelKey
	Start      TimeStamp
	Data       []byte
	fileKey    fileKey
	offset     int64
	size       Size
}

type SegmentHeader struct {
	ChannelKey ChannelKey
	Start      TimeStamp
	FileKey    fileKey
	Offset     int64
	Size       Size
}

func (sg Segment) Sugar(channel Channel, Bound TimeRange) SugaredSegment {
	return SugaredSegment{Segment: sg, Bound: Bound, Channel: channel}
}

func (sg Segment) Header() SegmentHeader {
	return SegmentHeader{ChannelKey: sg.ChannelKey, Start: sg.Start, FileKey: sg.fileKey, Offset: sg.offset, Size: sg.size}
}

func (sg SegmentHeader) Flush(w io.Writer) error { return binary.Flush(w, sg) }

func (sg *Segment) LoadHeader(header SegmentHeader) {
	sg.ChannelKey = header.ChannelKey
	sg.Start = header.Start
	sg.fileKey = header.FileKey
	sg.offset = header.Offset
	sg.size = header.Size
}

// Size returns the Size of the segment in bytes.
func (sg Segment) Size() int {
	l := len(sg.Data)
	if l == 0 {
		return int(sg.size)
	}
	return l
}

// Key implements allocate.Item.
func (sg Segment) Key() ChannelKey { return sg.ChannelKey }

const segmentKVPrefix = "cs/sg"

func (sg Segment) KVKey() []byte {
	key := kv.StaticCompositeKey(segmentKVPrefix, sg.ChannelKey, sg.Start)
	return key
}

func (sg Segment) flushData(w io.Writer) error { return binary.Write(w, sg.Data) }

// |||||| SUGARED ||||||

// SugaredSegment injects additional functionality into a Segment.
type SugaredSegment struct {
	Segment
	Channel
	Bound TimeRange
}

func (s SugaredSegment) Offset() int64 {
	return s.Segment.offset + int64(TimeSpan(s.Start()-s.Segment.Start).ByteSize(s.Channel.DataRate, s.Channel.DataType))
}

func (s SugaredSegment) Size() Size { return s.Span().ByteSize(s.Channel.DataRate, s.Channel.DataType) }

func (s SugaredSegment) Start() TimeStamp { return s.Range().Start }

func (s SugaredSegment) End() TimeStamp { return s.Range().End }

// Range returns the bounded range of the segment.
func (s SugaredSegment) Range() TimeRange { return s.UnboundedRange().BoundBy(s.Bound) }

// Span returns the bounded span of the segment.
func (s SugaredSegment) Span() TimeSpan { return s.Range().Span() }

func (s SugaredSegment) UnboundedRange() TimeRange {
	return s.Segment.Start.SpanRange(s.UnboundedSpan())
}

func (s SugaredSegment) UnboundedSpan() TimeSpan {
	return s.DataRate.SizeSpan(Size(s.Segment.Size()), s.DataType)
}

// |||||| ITERATOR ||||||

type kvIterator struct {
	kv.Iterator
	pos     TimeStamp
	channel Channel
	value   []SugaredSegment
	err     error
	rng     TimeRange
}

func newSegmentKVIterator(channel Channel, rng telem.TimeRange, kve kv.KV) *kvIterator {
	sg := &kvIterator{pos: rng.Start, channel: channel}
	kvIter := kve.IterRange(sg.stampKey(rng.Start), sg.stampKey(rng.End))
	sg.Iterator = kvIter
	return sg
}

// Next returns the next segment in the streamIterator. Returns a boolean indicating whether the streamIterator
// is pointing at a valid segment.
func (si *kvIterator) Next() bool {
	si.collectGarbage()
	if !si.Iterator.Next() {
		return false
	}
	si.loadValue()
	si.setBounds(si.valueStart(), TimeStampMax)
	si.autoUpdatePos()
	return true
}

// Position returns the current TimeStamp of the streamIterator.
func (si *kvIterator) Position() TimeStamp { return si.pos }

// First seeks the segment to the first Segment in the streamIterator. Returns true if the streamIterator is pointing
// to a valid Segment.
func (si *kvIterator) First() bool {
	si.collectGarbage()
	if !si.Iterator.First() {
		return false
	}
	si.loadValue()
	si.setBounds(si.Position(), TimeStampMax)
	return true
}

func (si *kvIterator) valueStart() TimeStamp { return si.value[0].Start() }

func (si *kvIterator) valueEnd() TimeStamp { return si.value[len(si.value)-1].End() }

// Last seeks the segment to the last Segment in the streamIterator. Returns true if the streamIterator is pointing
// to a valid Segment.
func (si *kvIterator) Last() bool {
	si.collectGarbage()
	if !si.Iterator.Last() {
		return false
	}
	si.loadValue()
	si.updatePos(si.rng.End)
	si.setBounds(TimeStampMax, si.Position())
	return true
}

// NextSpan reads the segments from the streamIterator position to the end of the span.
// Returns a boolean indicating whether the streamIterator is pointing to a valid Segment.
func (si *kvIterator) NextSpan(span TimeSpan) bool {
	si.collectGarbage()
	end := si.Position().Add(span)
	limit := si.stampKey(end)
	i := 0
	for {
		i++
		if state := si.Iterator.NextWithLimit(limit); state != pebble.IterValid {
			break
		}
		si.loadValue()
	}
	if si.IsZero() {
		return true
	}
	si.setBounds(si.Position(), end)
	si.autoUpdatePos()
	return true
}

// NextRange reads the segments in the provided range. Returns a boolean indicating whether the streamIterator
// is pointing to a valid Segment.
func (si *kvIterator) NextRange(tr telem.TimeRange) bool {
	if !si.SeekLT(tr.Start) {
		return false
	}
	return si.NextSpan(tr.Span())
}

// Value returns the current streamIterator value.
func (si *kvIterator) Value() []SugaredSegment { return si.value }

// SeekLT seeks the streamIterator to the first Segment with a timestamp less than or equal to the given stamp.
// Returns a boolean indicating whether the streamIterator is pointing at a valid Segment.
func (si *kvIterator) SeekLT(stamp TimeStamp) bool {
	si.collectGarbage()
	if !si.Iterator.SeekLT(si.stampKey(stamp)) {
		return false
	}
	si.updatePos(stamp)
	return true
}

// SeekGE seeks the streamIterator to the first Segment with a timestamp greater than or equal to the
// Returns a boolean indicating whether the streamIterator is pointing at a valid Segment.
func (si *kvIterator) SeekGE(stamp TimeStamp) bool {
	si.collectGarbage()
	if !si.Iterator.SeekGE(si.stampKey(stamp)) {
		return false
	}
	si.updatePos(stamp)
	return true
}

// IsZero returns true if the streamIterator value contains any segments.
func (si *kvIterator) IsZero() bool { return len(si.value) == 0 }

// stampKey returns the key for a particular TimeStamp.
func (si *kvIterator) stampKey(stamp TimeStamp) []byte {
	return Segment{Start: stamp, ChannelKey: si.channel.Key}.KVKey()
}

// loadValue loads a SugaredSegment from the current streamIterator value. Assumes the streamIterator is valid.
func (si *kvIterator) loadValue() {
	seg := &Segment{}
	header := seg.Header()
	if err := binary.Load(bytes.NewBuffer(si.Iterator.Value()), &header); err != nil {
		si.writeError(err)
	}
	seg.LoadHeader(header)
	si.value = append(si.value, seg.Sugar(si.channel, TimeRangeMax))
}

func (si *kvIterator) setBounds(lower TimeStamp, upper TimeStamp) {
	si.value[0].Bound.Start = lower
	si.value[len(si.value)-1].Bound.End = upper
}

// autoUpdatePos updates the streamIterator position to the bounded end timestamp of the last segment in the value.
// assumes the streamIterator is valid.
func (si *kvIterator) autoUpdatePos() { si.pos = si.valueEnd() }

// updatePos updates the position of the streamIterator.
func (si *kvIterator) updatePos(stamp TimeStamp) { si.pos = stamp }

// writeError writes sets the error for the current iteration.
func (si *kvIterator) writeError(err error) { si.err = err }

// collectGarbage removes any segments from the value and sets the streamIterator error to nil.
func (si *kvIterator) collectGarbage() {
	si.value = []SugaredSegment{}
	si.err = nil
}

// |||||| KV ||||||

func Sort(segments []Segment) {
	sort.Slice(segments, func(i, j int) bool { return segments[i].Start.Before(segments[j].Start) })
}

type segmentKV struct {
	kv kv.KV
}

func (sk segmentKV) set(s Segment) error {
	return kv.Flush(sk.kv, s.KVKey(), s.Header())
}

func generateRangeKeys(cpk ChannelKey, tr TimeRange) ([]byte, []byte) {
	s, err := kv.CompositeKey(segmentKVPrefix, cpk, tr.Start)
	if err != nil {
		panic(err)
	}
	e, err := kv.CompositeKey(segmentKVPrefix, cpk, tr.End)
	if err != nil {
		panic(err)
	}
	return s, e
}
