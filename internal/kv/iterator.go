package kv

import (
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/telem"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Iterator is used to iterate over a channel's segment. Iterator stores its values as a bounded range of segments.
// Iterator is not goroutine safe, but it is safe to open several iterators over the same data. Iterator segment metadata
// is NOT guaranteed to be contiguous (i.e. there may be a gap occupying a particular sub-range), but it is guaranteed
// to be sequential.
type Iterator struct {
	internal *gorp.KVIterator[segment.Header]
	// position represents the iterators current 'view' of the time range (i.e. what sub-range will calls to Value return).
	// position is guaranteed to be within the constraint range, but is not guaranteed to contain valid data.
	// position  may zero span (i.e. position.IsZero() is true).
	position telem.TimeRange
	channel  channel.Channel
	value    *segment.Range
	rng      telem.TimeRange
	_error   error
}

// NewIterator opens a new Iterator over the specified time range of a channel.
func NewIterator(kve kv.KV, chKey channel.Key, rng telem.TimeRange) (iter *Iterator) {
	iter = &Iterator{rng: telem.TimeRangeMax}

	var err error
	iter.channel, err = NewChannel(kve).Get(chKey)
	if err != nil {
		iter.setError(err)
		return iter
	}

	iter.internal = gorp.WrapKVIter[segment.Header](kve.IterPrefix(segment.NewKeyPrefix(chKey)))

	start, end := iter.key(rng.Start).Bytes(), iter.key(rng.End).Bytes()

	if iter.SeekLT(rng.Start) && iter.Next() && iter.Value().Range().Overlap(rng) {
		start = iter.key(iter.Value().UnboundedRange().Start).Bytes()
	} else if iter.SeekGE(rng.Start) && iter.Next() && iter.Value().Range().Overlap(rng) {
		start = iter.key(iter.Value().UnboundedRange().Start).Bytes()
	} else {
		iter.setError(errors.New("[cesium.kv] - range has no data"))
	}

	if iter.SeekGE(rng.End) && iter.Prev() && iter.Value().Range().Overlap(rng) {
		iter.loadValue()
		end = iter.key(iter.Value().UnboundedRange().End).Bytes()
	} else if iter.SeekLT(rng.End) && iter.Next() && iter.Value().Range().Overlap(rng) {
		iter.loadValue()
		end = iter.key(iter.Value().UnboundedRange().End).Bytes()
	} else {
		iter.setError(errors.New("[cesium.kv] - range has no data"))
	}

	iter.rng = rng
	iter.setError(iter.internal.Close())
	iter.internal = gorp.WrapKVIter[segment.Header](kve.IterRange(start, end))

	return iter
}

// First moves to the first segment in the iterator. Sets the iterator position to the range
// of the first segment. Returns true if the iterator is pointing at a valid segment.
func (i *Iterator) First() bool {
	if i.error() != nil {
		return false
	}
	i.resetValue()

	if !i.internal.First() {
		return false
	}

	i.loadValue()
	i.updatePosition(i.Value().UnboundedRange())
	i.boundValueToPosition()
	return true
}

// Last moves to the last segment in the iterator. Sets the iterator position to the range of the last segment.
// Returns true if the iterator is pointing to a valid segment.
func (i *Iterator) Last() bool {
	if i.error() != nil {
		return false
	}
	i.resetValue()

	if !i.internal.Last() {
		return false
	}

	i.loadValue()
	i.updatePosition(i.Value().UnboundedRange())
	i.boundValueToPosition()
	return true
}

// Next moves to the next segment in the iterator. Sets the iterator position to the range of the segment.
// If the segment exceeds the range of the iterator, bounds the position by the global range.
func (i *Iterator) Next() bool {
	if i.error() != nil {
		return false
	}
	i.resetValue()

	// If we're at a zero position in the iterator, it means we just completed a seek operation. Instead, we
	// just load the value at the current position and update our position to span it.
	if !i.Position().IsZero() && !i.internal.Next() {
		return false
	}

	i.loadValue()
	i.updatePosition(i.value.UnboundedRange())
	i.boundValueToPosition()
	return true
}

// Prev moves to the next segment in the iterator. Sets the iterator position to the range of the segment.
//  If the segment exceeds the range of the iterator, bounds teh position by the global range.
func (i *Iterator) Prev() bool {
	if i.error() != nil {
		return false
	}
	i.resetValue()

	if !i.Position().IsZero() && !i.internal.Prev() {
		return false
	}

	i.loadValue()
	i.updatePosition(i.value.UnboundedRange())
	i.boundValueToPosition()

	return true
}

// NextSpan moves the iterator across the provided span, loading any segments in encountered. Returns true if ANY valid
// data within the span is encountered. The data doesn't have to be contiguous for NextSpan to return true.
func (i *Iterator) NextSpan(span telem.TimeSpan) bool {
	if i.error() != nil {
		return false
	}
	var (
		rng    = i.Position().End.SpanRange(span)
		endKey = i.key(rng.End).Bytes()
	)

	if rng.Overlap(i.value.UnboundedRange()) {
		i.resetValue()
		i.loadValue()
	} else {
		i.resetValue()
	}

	for {
		if state := i.internal.NextWithLimit(endKey); state != pebble.IterValid {
			break
		}
		i.loadValue()
	}

	i.updatePosition(rng)
	i.boundValueToPosition()

	return true
}

// NextRange seeks the iterator to the start of the provided range and moves it across, loading any segments encountered.
// Returns true if ANY valid data within the range is encountered. THe data doesn't have to be contiguous for NextRange
// to return true.
func (i *Iterator) NextRange(tr telem.TimeRange) bool {
	tr = tr.BoundBy(i.rng)

	// First we try blindly seeking to the start of the range. If that works,
	// we can simply return all the data in that span.
	if i.Seek(tr.Start) {
		return i.NextSpan(tr.Span())
	}

	// If not we seek the first segment whose values may fall within the range.
	if !i.SeekGE(tr.Start) {
		return false
	}

	i.loadValue()

	// If the range of the value we found doesn't overlap with the range we're
	// looking for then we return false.
	if !i.value.UnboundedRange().Overlap(tr) {
		return false
	}

	i.resetValue()
	i.NextSpan(telem.TimeRange{Start: i.Position().Start, End: tr.End}.Span())
	i.updatePosition(tr)
	i.boundValueToPosition()

	return true
}

// SeekFirst seeks the iterator to the first valid segment. Returns true if the iterator is pointing at a valid segment. Sets
// the iterator position to a zero spanned range starting and ending at the start of the first segment.
// Calls to Iterator.Value will return an invalid range. Next(), NextRange(), or NextSpan() must be called to validate
// the iterator.
func (i *Iterator) SeekFirst() bool {
	i.resetError()
	i.resetValue()

	if !i.internal.First() {
		return false
	}

	i.loadValue()
	i.updatePosition(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// SeekLast seeks the iterator to the last valid segment. Returns true if the iterator is pointing at a valid segment. Sets
// the iterator position to a zero spanned range starting and ending at the start of the first segment.
// Calls to Iterator.Value will return an invalid range. Next(), NextRange(), or NextSpan() must be called to validate
// the iterator.
func (i *Iterator) SeekLast() bool {
	i.resetError()
	i.resetValue()

	if !i.internal.Last() {
		return false
	}

	i.loadValue()
	i.updatePosition(i.value.UnboundedRange().End.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// SeekLT seeks the iterator to the first segment whose start timestamp is less than the provided stamp.
// Returns true if the iterator is pointing at a valid segment. Sets the iterator position to a zero spanned
// range starting and ending at the start of the segment.
// Calls to Iterator.Value will return an invalid range. Next(), NextRange(), or NextSpan() must be called
// to validate the iterator.
func (i *Iterator) SeekLT(stamp telem.TimeStamp) bool {
	i.resetError()
	if !i.rng.ContainsStamp(stamp) {
		i.updatePosition(stamp.SpanRange(0))
		return false
	}

	i.resetValue()

	if !i.internal.SeekLT(i.key(stamp).Bytes()) {
		return false
	}

	i.loadValue()
	i.updatePosition(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// SeekGE seeks the iterator to the first segment whose start timestamp is higher than the provided stamp.
// Returns true if the iterator is pointing at a valid segment. Sets the iterator position to a zero spanned
// range starting and ending at the start of the segment. Calls to Iterator.Value will return an invalid range. Next(),
// NextRange(), or NextSpan() must be called to validate the iterator.
func (i *Iterator) SeekGE(stamp telem.TimeStamp) bool {
	i.resetError()
	if !i.rng.ContainsStamp(stamp) {
		i.updatePosition(stamp.SpanRange(0))
		return true
	}

	i.resetValue()

	if !i.internal.SeekGE(i.key(stamp).Bytes()) {
		return false
	}

	i.loadValue()
	i.updatePosition(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// Seek seeks the iterator to the provided timestamp. Returns true if the iterator is pointing at a valid segment. Sets
// the iterator position to a zero spanned range starting and ending at the provided stamp. Calls to Iterator.Value
// will return an invalid range. Next(), NextRange(), or NextSpan() must be called to validate the iterator.
func (i *Iterator) Seek(stamp telem.TimeStamp) bool {

	// We need to seek the first value whose data might contain our timestamp. If it does, we're pointing at a valid
	// segment and can return true. If the seekLT is invalid, it means we definitely can't find a valid segment.
	if ok := i.SeekLT(stamp); !ok {
		return false
	}

	i.loadValue()

	// If the range of the value we looked for doesn't contain our stamp, it means a segment with data at that timestamp
	// doesn't exist, so we return false.
	if !i.value.UnboundedRange().ContainsStamp(stamp) {
		return false
	}

	i.resetValue()
	i.updatePosition(stamp.SpanRange(0))
	i.boundValueToPosition()

	return true
}

func (i *Iterator) Value() *segment.Range { return i.value }

func (i *Iterator) Position() telem.TimeRange { return i.position }

func (i *Iterator) key(stamp telem.TimeStamp) segment.Key {
	return segment.NewKey(i.channel.Key, stamp)
}

func (i *Iterator) updatePosition(rng telem.TimeRange) { i.position = rng.BoundBy(i.rng) }

func (i *Iterator) loadValue() { i.value.Headers = append(i.value.Headers, i.internal.Value()) }

func (i *Iterator) boundValueToPosition() { i.value.Bound = i.Position() }

func (i *Iterator) resetValue() {
	i.value = new(segment.Range)
	i.value.Bound = telem.TimeRangeMax
	i.value.Channel = i.channel
}

func (i *Iterator) setError(err error) {
	if err != nil {
		i._error = err
	}
}

func (i *Iterator) Close() error {
	i.resetValue()
	return i.internal.Close()
}

func (i *Iterator) resetError() { i._error = nil }

func (i *Iterator) Error() error {
	if i.error() != nil {
		return i.error()
	}
	return i.internal.Error()
}

func (i *Iterator) error() error { return i._error }

func (i *Iterator) Valid() bool {
	if i.error() != nil {
		return false
	}
	if i.Position().IsZero() {
		return false
	}
	return i.internal.Valid()
}
