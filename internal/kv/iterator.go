package kv

import (
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/telem"
	"github.com/cockroachdb/errors"
)

// Iterator is used to iterate over a channel's data. Iterator stores its values as a bounded range of segment headers.
// Iterator is not goroutine safe, but it is safe to open several iterators over the same data. Iterator segment metadata
// is NOT guaranteed to be contiguous (i.e. there may be a gap occupying a particular sub-range), but it is guaranteed
// to be sequential. In the case of backwards iteration, loaded segments are in rever
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
		end = iter.key(iter.Value().UnboundedRange().End).Bytes()
	} else if iter.SeekLT(rng.End) && iter.Next() && iter.Value().Range().OverlapsWith(rng) {
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

	i.appendValue()
	i.updateView(i.Value().UnboundedRange())
	i.boundValueToView()
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

	i.appendValue()
	i.updateView(i.Value().UnboundedRange())
	i.boundValueToView()
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

	i.appendValue()
	i.updateView(i.value.UnboundedRange())
	i.boundValueToView()
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

	i.appendValue()
	i.updateView(i.value.UnboundedRange())
	i.boundValueToView()

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
		i.appendValue()
	}

	// If the current value can't satisfy the range, we need to continue loading in segments until it does.
	if !i.Value().UnboundedRange().End.After(rng.End) {
		for i.internal.Next(); i.Value().UnboundedRange().End.Before(rng.End) && i.internal.Valid(); i.internal.Next() {
			i.appendValue()
		}
	}

	i.updateView(rng)
	i.boundValueToView()

	// In the edge case that we're crossing over the global range boundary, we need to run some special logic ->
	// If we don't have a non-internal-iter error, we weren't already at the end before this call, and we have
	// a valid value otherwise, then we need to force the internal kv.Iterator to be valid until the next movement
	// call.
	if rng.End.After(i.rng.End) &&
		i.Error() == nil &&
		prevRange.End != i.rng.End &&
		!i.Value().Empty() {
		i.forceInternalValid()
	}

	return !i.Value().Empty()
}

// PrevSpan moves the iterator backwards across the provided span, loading any segments encountered. Returns true if ANY
// valid data within the span is encountered. The segments don't have to be contiguous for PrevSpan to return true.
func (i *Iterator) PrevSpan(span telem.TimeSpan) bool {
	if i.error() != nil {
		return false
	}
	i.resetForceInternalValid()
	i.resetValue()

	rng := i.View().Start.SpanRange(-span)
	prevView := i.View()

	if !i.SeekLT(i.View().Start) {
		return false
	}

	if !i.Value().UnboundedRange().OverlapsWith(rng) {
		i.resetValue()
	}

	if !i.Value().UnboundedRange().Start.After(rng.Start) {
		for i.internal.Prev(); i.Value().UnboundedRange().Start.After(rng.Start) && i.internal.Valid(); i.internal.Prev() {
			i.prependValue()
		}
	}

	i.updateView(rng)
	i.boundValueToView()

	// If our iterator isn't valid, it means we've reached the first segment in the global range. If our unbounded
	// view from the previous iteration ISN'T already at the start, we need to force the iterator to be valid.
	if !i.internal.Valid() &&
		i.Error() == nil &&
		prevView.Start != i.rng.Start &&
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

	// If the range of the value we found doesn't overlap with the range we're
	// looking for then we return false.
	if !i.value.UnboundedRange().OverlapsWith(tr) {
		return false
	}

	i.NextSpan(telem.TimeRange{Start: i.View().Start, End: tr.End}.Span())
	i.updateView(tr)
	i.boundValueToView()

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

	i.appendValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToView()

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

	i.appendValue()
	i.updateView(i.value.UnboundedRange().End.SpanRange(0))
	i.boundValueToView()

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

	i.appendValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToView()

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

	i.appendValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToView()

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

	i.appendValue()

	// If the range of the value we looked for doesn't contain our stamp, it means a segment with data at that timestamp
	// doesn't exist, so we return false.
	if !i.value.UnboundedRange().ContainsStamp(stamp) {
		return false
	}

	i.updateView(stamp.SpanRange(0))
	i.boundValueToView()

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

// Valid returns true if the iterator View is looking at valid segments. Will return false after any seek operations.
func (i *Iterator) Valid() bool {
	if i.error() != nil || i.View().IsZero() {
		return false
	}
	if i._forceInternalValid {
		return true
	}
	return i.internal.Valid()
}

// Close closes the iterator.
func (i *Iterator) Close() error {
	i.resetValue()
	return i.internal.Close()
}

func (i *Iterator) key(stamp telem.TimeStamp) segment.Key {
	return segment.NewKey(i.channel.Key, stamp)
}

func (i *Iterator) updateView(rng telem.TimeRange) { i.view = rng.BoundBy(i.rng) }

func (i *Iterator) appendValue() { i.value.Headers = append(i.value.Headers, i.internal.Value()) }

func (i *Iterator) prependValue() {
	i.value.Headers = append([]segment.Header{i.internal.Value()}, i.value.Headers...)
}

func (i *Iterator) boundValueToView() { i.value.Bound = i.View() }

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

func (i *Iterator) resetError() { i._error = nil }

func (i *Iterator) error() error { return i._error }
