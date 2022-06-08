package kv

import (
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/telem"
	"github.com/cockroachdb/errors"
)

// Iterator is used to iterate over a channel's segment. Iterator stores its values as a bounded range of segments.
// Iterator is not goroutine safe, but it is safe to open several iterators over the same data. Iterator segment metadata
// is NOT guaranteed to be contiguous (i.e. there may be a gap occupying a particular sub-range), but it is guaranteed
// to be sequential.
type Iterator struct {
	internal *gorp.KVIterator[segment.Header]
	// view represents the iterators current 'view' of the time range (i.e. what sub-range will calls to Value return).
	// view is guaranteed to be within the constraint range, but is not guaranteed to contain valid data.
	// view  may zero span (i.e. view.IsZero() is true).
	view                telem.TimeRange
	channel             channel.Channel
	value               *segment.Range
	rng                 telem.TimeRange
	_error              error
	_forceInternalValid bool
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

	if iter.SeekLT(rng.Start) && iter.Next() && iter.Value().Range().OverlapsWith(rng) {
		start = iter.key(iter.Value().UnboundedRange().Start).Bytes()
	} else if iter.SeekGE(rng.Start) && iter.Next() && iter.Value().Range().OverlapsWith(rng) {
		start = iter.key(iter.Value().UnboundedRange().Start).Bytes()
	} else {
		iter.setError(errors.New("[cesium.kv] - range has no data"))
	}

	if iter.SeekGE(rng.End) && iter.Prev() && iter.Value().Range().OverlapsWith(rng) {
		iter.loadValue()
		end = iter.key(iter.Value().UnboundedRange().End).Bytes()
	} else if iter.SeekLT(rng.End) && iter.Next() && iter.Value().Range().OverlapsWith(rng) {
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

// First moves to the first segment in the iterator. Sets the iterator view to the range
// of the first segment. Returns true if the iterator is pointing at a valid segment.
func (i *Iterator) First() bool {
	i.resetError()
	i.resetValue()
	i.resetForceInternalValid()

	if !i.internal.First() {
		return false
	}

	i.loadValue()
	i.updateView(i.Value().UnboundedRange())
	i.boundValueToPosition()
	return true
}

// Last moves to the last segment in the iterator. Sets the iterator view to the range of the last segment.
// Returns true if the iterator is pointing to a valid segment.
func (i *Iterator) Last() bool {
	i.resetError()
	i.resetValue()
	i.resetForceInternalValid()

	if !i.internal.Last() {
		return false
	}

	i.loadValue()
	i.updateView(i.Value().UnboundedRange())
	i.boundValueToPosition()
	return true
}

// Next moves to the next segment in the iterator. Sets the iterator view to the range of the segment.
// If the segment exceeds the range of the iterator, bounds the view by the global range.
func (i *Iterator) Next() bool {
	if i.error() != nil {
		return false
	}
	i.resetValue()
	i.resetForceInternalValid()

	// If we're at a zero view in the iterator, it means we just completed a seek operation. Instead, we
	// just load the value at the current view and update our view to span it. BUT if we're at the
	// end of the range, still call next to invalidate it.
	if (!i.View().IsZero() || i.View().End == i.rng.End) && !i.internal.Next() {
		return false
	}

	i.loadValue()
	i.updateView(i.value.UnboundedRange())
	i.boundValueToPosition()
	return true
}

// Prev moves to the next segment in the iterator. Sets the iterator view to the range of the segment.
//  If the segment exceeds the range of the iterator, bounds teh view by the global range.
func (i *Iterator) Prev() bool {
	if i.error() != nil {
		return false
	}
	i.resetValue()
	i.resetForceInternalValid()

	if (!i.View().IsZero() || i.View().Start == i.rng.Start) && !i.internal.Prev() {
		return false
	}

	i.loadValue()
	i.updateView(i.value.UnboundedRange())
	i.boundValueToPosition()

	return true
}

// NextSpan moves the iterator across the provided span, loading any segments in encountered. Returns true if ANY valid
// data within the span is encountered. The data doesn't have to be contiguous for NextSpan to return true.
func (i *Iterator) NextSpan(span telem.TimeSpan) bool {
	if i.error() != nil {
		return false
	}
	i.resetForceInternalValid()

	rng := i.View().End.SpanRange(span)
	prevRange := i.value.UnboundedRange()
	i.resetValue()

	// If the last segment from the previous value had relevant data, we need to load it back in.
	if rng.OverlapsWith(prevRange) {
		i.loadValue()
	}

	// If the current value can't satisfy the range, we need to continue loading in segments until it does.
	if !i.Value().UnboundedRange().End.After(rng.End) {
		for i.internal.Next(); i.Value().UnboundedRange().End.Before(rng.End) && i.internal.Valid(); i.internal.Next() {
			i.loadValue()
		}
	}

	i.updateView(rng)
	i.boundValueToPosition()

	// In the edge case that we're crossing over the global range boundary, we need to run some special logic ->
	// If we don't have a non-internal-iter error, we weren't already at the end before this call, and we have
	// a valid value otherwise, then we need to force the internal kv.Iterator to be valid until the next movement
	// call.
	if rng.End.After(i.rng.End) &&
		i.error() == nil &&
		prevRange.End != i.rng.End &&
		!i.Value().Empty() {
		i.forceInternalValid()
	}

	return !i.Value().Empty()
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
	if !i.value.UnboundedRange().OverlapsWith(tr) {
		return false
	}

	i.resetValue()
	i.NextSpan(telem.TimeRange{Start: i.View().Start, End: tr.End}.Span())
	i.updateView(tr)
	i.boundValueToPosition()

	return true
}

// SeekFirst seeks the iterator to the first valid segment. Returns true if the iterator is pointing at a valid segment. Sets
// the iterator view to a zero spanned range starting and ending at the start of the first segment.
// Calls to Iterator.Value will return an invalid range. Next(), NextRange(), or NextSpan() must be called to validate
// the iterator.
func (i *Iterator) SeekFirst() bool {
	i.resetError()
	i.resetValue()
	i.resetForceInternalValid()

	if !i.internal.First() {
		return false
	}

	i.loadValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// SeekLast seeks the iterator to the last valid segment. Returns true if the iterator is pointing at a valid segment. Sets
// the iterator view to a zero spanned range starting and ending at the start of the first segment.
// Calls to Iterator.Value will return an invalid range. Next(), NextRange(), or NextSpan() must be called to validate
// the iterator.
func (i *Iterator) SeekLast() bool {
	i.resetError()
	i.resetValue()
	i.resetForceInternalValid()

	if !i.internal.Last() {
		return false
	}

	i.loadValue()
	i.updateView(i.value.UnboundedRange().End.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// SeekLT seeks the iterator to the first segment whose start timestamp is less than the provided stamp.
// Returns true if the iterator is pointing at a valid segment. Sets the iterator view to a zero spanned
// range starting and ending at the start of the segment. Calls to Iterator.Value will return an invalid range.
// Next(), NextRange(), or NextSpan() must be called to validate the iterator.
func (i *Iterator) SeekLT(stamp telem.TimeStamp) bool {
	i.resetError()
	i.resetForceInternalValid()
	if !i.rng.ContainsStamp(stamp) {
		i.updateView(stamp.SpanRange(0))
		return false
	}

	i.resetValue()

	if !i.internal.SeekLT(i.key(stamp).Bytes()) {
		return false
	}

	i.loadValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// SeekGE seeks the iterator to the first segment whose start timestamp is higher than the provided stamp.
// Returns true if the iterator is pointing at a valid segment. Sets the iterator view to a zero spanned
// range starting and ending at the start of the segment. Calls to Iterator.Value will return an invalid range. Next(),
// NextRange(), or NextSpan() must be called to validate the iterator.
func (i *Iterator) SeekGE(stamp telem.TimeStamp) bool {
	i.resetError()
	i.resetForceInternalValid()
	if !i.rng.ContainsStamp(stamp) {
		i.updateView(stamp.SpanRange(0))
		return true
	}

	i.resetValue()

	if !i.internal.SeekGE(i.key(stamp).Bytes()) {
		return false
	}

	i.loadValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// Seek seeks the iterator to the provided timestamp. Returns true if the iterator is pointing at a valid segment. Sets
// the iterator view to a zero spanned range starting and ending at the provided stamp. Calls to Iterator.Value
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

	i.updateView(stamp.SpanRange(0))
	i.boundValueToPosition()

	return true
}

// Value returns the current Range stored in the iterator. The Range is not guaranteed to be continuous, but it is
// guaranteed to be sequential. It's important to note that the TimeRange returned by Value().Range() will either be
// equal to or be a sub-range of the current View(). On the other hand, the TimeRange returned by Value().UnboundedRange()
// is either equal to or a super-range of Value().Range() (meaning it can be larger than the current View()).
func (i *Iterator) Value() *segment.Range { return i.value }

// View returns a TimeRange representing the iterators current 'view' of the data i.e. what is the potential range of
// segments currently loaded into Value().
func (i *Iterator) View() telem.TimeRange { return i.view }

// Error returns any errors encountered by the iterator.
func (i *Iterator) Error() error {
	if i.error() != nil {
		return i.error()
	}
	return i.internal.Error()
}

func (i *Iterator) Valid() bool {
	if i.error() != nil || i.View().IsZero() {
		return false
	}
	if i._forceInternalValid {
		return true
	}
	return i.internal.Valid()
}

func (i *Iterator) key(stamp telem.TimeStamp) segment.Key {
	return segment.NewKey(i.channel.Key, stamp)
}

func (i *Iterator) updateView(rng telem.TimeRange) { i.view = rng.BoundBy(i.rng) }

func (i *Iterator) loadValue() { i.value.Headers = append(i.value.Headers, i.internal.Value()) }

func (i *Iterator) boundValueToPosition() { i.value.Bound = i.View() }

func (i *Iterator) resetValue() {
	i.value = new(segment.Range)
	i.value.Bound = telem.TimeRangeMax
	i.value.Channel = i.channel
}

func (i *Iterator) forceInternalValid() { i._forceInternalValid = true }

func (i *Iterator) resetForceInternalValid() { i._forceInternalValid = false }

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

func (i *Iterator) error() error { return i._error }
