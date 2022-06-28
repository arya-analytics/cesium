package kv

import (
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/query"
	"github.com/arya-analytics/x/telem"
	"github.com/cockroachdb/errors"
)

// Iterator is used to iterate over one or more channel's data.
// Iterator stores its values as a bounded range of segment headers.
// Iterator is not goroutine safe, but it is safe
// to open several iterators over the same data. unaryIterator segment metadata
// is NOT guaranteed to be contiguous (i.e. there may be a gap occupying a particular
// sub-range), but it is guaranteed to be sequential. In the case of backwards iteration,
// loaded segments are in reverse order.
type Iterator interface {
	// First moves to the first segment in the iterator. Sets the iterator view to the range
	// of the first segment. Returns true if the iterator is pointing at a valid segment.
	First() bool
	// Last moves to the last segment in the iterator. Sets the iterator view to the
	// range of the last segment. Returns true if the iterator is pointing to a valid segment.
	Last() bool
	// Next moves to the next segment in the iterator. Sets the iterator view to the
	// range of the segment. If the segment exceeds the range of the iterator, bounds
	// the view by the global range.
	Next() bool
	// Prev moves to the next segment in the iterator. Sets the iterator view to the
	// range of the segment. If the segment exceeds the range of the iterator, bounds
	// the view by the global range.
	Prev() bool
	// NextSpan moves the iterator across the provided span, loading any segments in
	// encountered. Returns true if ANY valid data within the span is encountered.
	// The data doesn't have to be contiguous for NextSpan to return true.
	NextSpan(span telem.TimeSpan) bool
	// PrevSpan moves the iterator backwards across the provided span, loading any
	// segments encountered. Returns true if ANY valid data within the span is
	// encountered. The segments don't have to be contiguous for PrevSpan to return
	// true.
	PrevSpan(span telem.TimeSpan) bool
	// NextRange seeks the iterator to the start of the provided range and moves it
	// across, loading any segments encountered. Returns true if ANY valid data within
	// the range is encountered. THe data doesn't have to be contiguous for NextRange
	// to return true.
	NextRange(tr telem.TimeRange) bool
	// SeekFirst seeks the iterator to the first valid segment. Returns true if the
	// iterator is pointing at a valid segment. Sets the iterator view to a zero
	// spanned range starting and ending at the start of the first segment.
	// Calls to unaryIterator.Value will return an invalid range.
	// Next(), NextRange(), or NextSpan() must be called to validate
	// the iterator.
	SeekFirst() bool
	// SeekLast seeks the iterator to the last valid segment. Returns true if the
	// iterator is pointing at a valid segment. Sets the iterator view to a zero
	// spanned range starting and ending at the start of the first segment.
	// Calls to unaryIterator.Value will return an invalid range. Next(), NextRange(),
	// or NextSpan() must be called to validate the iterator.
	SeekLast() bool
	// SeekLT seeks the iterator to the first segment whose start timestamp is less
	// than the provided stamp. Returns true if the iterator is pointing at a
	// valid segment. Sets the iterator view to a zero spanned range starting and ending
	// at the start of the segment. Calls to unaryIterator.
	// Value will return an invalid range. Next(), NextRange(), or NextSpan()
	// must be called to validate the iterator.
	SeekLT(stamp telem.TimeStamp) bool
	// SeekGE seeks the iterator to the first segment whose start timestamp
	// is higher than the provided stamp. Returns true if the iterator is pointing
	// at a valid segment. Sets the iterator view to a zero spanned
	// range starting and ending at the start of the segment. Calls to
	// unaryIterator.Value will return an invalid range. Next(),
	// NextRange(), or NextSpan() must be called to validate the iterator.
	SeekGE(stamp telem.TimeStamp) bool
	// Seek seeks the iterator to the provided timestamp. Returns true if the iterator
	// is pointing at a valid segment. Sets the iterator view to a zero spanned
	// range starting and ending at the provided stamp. Calls to unaryIterator.Value
	// will return an invalid range. Next(), NextRange(),
	// or NextSpan() must  be called to validate the iterator.
	Seek(stamp telem.TimeStamp) bool
	// Values returns the ranges underneath the iterators current View. The ranges
	// are not guaranteed to be continuous, but are guaranteed to be sequential.
	// It's important to note that the TimeRange returned by Value().BoundedRange(
	// will either be equal to or be a sub-range of the current View(). On the other
	// hand, the TimeRange returned by Value().UnboundedRange() is either equal to
	// or a super-range of Value().BoundedRange() ( meaning it can be larger than
	// the current View()).
	Values() []*segment.Range
	// Value returns the range for  the first channel in the iterator. Returns
	// a nil range if the channel does not exist in the iterator. This is useful for
	// iterating over a single channel.
	Value() *segment.Range
	// GetValue returns the range for the provided channel key. Returns a nil range
	// if the channel does not exist in the iterator. Passing a channel key of 0
	// is identical to calling Value().
	GetValue(key channel.Key) *segment.Range
	// View returns a TimeRange representing the iterators current 'view' of the data
	// i.e. what is the potential range of segments currently loaded into Value().
	View() telem.TimeRange
	// Error returns any errors encountered by the iterator. Error is reset after any
	// non-relative call (i.e. First(), Last(), SeekFirst(), SeekLast(), SeekLT(),
	//SeekGE(), Seek()). If Error is non-nil, any calls to Next(), NextRange(), NextSpan(),
	// Prev(), PrevSpan(), or Valid() will return false.
	Error() error
	// Valid returns true if the iterator View is looking at valid segments.
	// Will return false after any seek operations.
	Valid() bool
	// Close closes the iterator.
	Close() error
}

// NewIterator opens a new unaryIterator over the specified time range of a channel.
func NewIterator(kve kv.KV, rng telem.TimeRange,
	keys ...channel.Key) (iter Iterator) {
	if len(keys) == 0 {
		panic("[cesium.kv] - NewIterator() called with no keys")
	}
	if len(keys) == 1 {
		return newUnaryIterator(kve, rng, keys[0])
	}
	var compound compoundIterator
	for _, k := range keys {
		compound = append(compound, *newUnaryIterator(kve, rng, k))
	}
	return &compound
}

type unaryIterator struct {
	internal *gorp.KVIterator[segment.Header]
	// view represents the iterators current 'view' of the time range (i.e. what
	// sub-range will calls to Value return). view is guaranteed to be within
	// the constraint range, but is not guaranteed to contain valid data.
	// view may have zero span (i.e. view.IsZero() is true).
	view                telem.TimeRange
	channel             channel.Channel
	value               *segment.Range
	rng                 telem.TimeRange
	_error              error
	_forceInternalValid bool
}

// NewIterator opens a new unaryIterator over the specified time range of a channel.
func newUnaryIterator(kve kv.KV, rng telem.TimeRange, key channel.Key) (iter *unaryIterator) {
	iter = &unaryIterator{rng: telem.TimeRangeMax}

	chs, err := NewChannel(kve).Get(key)
	if err != nil {
		iter.setError(err)
		return iter
	}
	if len(chs) == 0 {
		iter.setError(query.NotFound)
		return iter
	}

	iter.channel = chs[0]

	iter.internal = gorp.WrapKVIter[segment.Header](kve.IterPrefix(segment.NewKeyPrefix(key)))

	start, end := iter.key(rng.Start).Bytes(), iter.key(rng.End).Bytes()

	// Seek to the first segment that starts before the start of the range. If the
	// segment range overlaps with our desired range, we'll use it as the starting
	// point for the iterator. Otherwise, we'll seek to the first segment that starts
	// after the start of the range. If this value overlaps with our desired range,
	// we'll use it as the starting point for the iterator. Otherwise, set an error
	// on the iterator/
	if iter.SeekLT(rng.Start) && iter.Next() && iter.Value().Range().OverlapsWith(
		rng) {
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

// First implements Iterator.
func (i *unaryIterator) First() bool {
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

// Last implements Iterator.
func (i *unaryIterator) Last() bool {
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

// Next implements Iterator.
func (i *unaryIterator) Next() bool {
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

// Prev implements Iterator.
func (i *unaryIterator) Prev() bool {
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

// NextSpan implements Iterator.
func (i *unaryIterator) NextSpan(span telem.TimeSpan) bool {
	if span < 0 {
		return i.PrevSpan(-1 * span)
	}
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
		for i.internal.Next(); i.Value().UnboundedRange().End.Before(rng.End) && i.
			internal.Valid(); i.internal.Next() {
			i.appendValue()
		}
	}

	i.updateView(rng)
	i.boundValueToView()

	// In the edge case that we're crossing over the global range boundary, we need to run some special logic ->
	// If we don't have a non-internal-iter error, we weren't already at the end before this call, and we have
	// a valid value otherwise, then we need to force the internal kv.Iterate to be valid until the next movement
	// call.
	if rng.End.After(i.rng.End) &&
		i.Error() == nil &&
		prevRange.End != i.rng.End &&
		!i.Value().Empty() {
		i.forceInternalValid()
	}

	return !i.Value().Empty()
}

// PrevSpan implements Iterator.
func (i *unaryIterator) PrevSpan(span telem.TimeSpan) bool {
	if span < 0 {
		return i.NextSpan(-1 * span)
	}
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

	if i.Value().UnboundedRange().Start.After(rng.Start) {
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

// NextRange implements Iterator.
func (i *unaryIterator) NextRange(tr telem.TimeRange) bool {
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

// SeekFirst implements Iterator.
func (i *unaryIterator) SeekFirst() bool {
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

// SeekLast implements Iterator.
func (i *unaryIterator) SeekLast() bool {
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

// SeekLT implements Iterator.
func (i *unaryIterator) SeekLT(stamp telem.TimeStamp) bool {
	i.resetError()
	i.resetForceInternalValid()

	i.resetValue()

	if !i.internal.SeekLT(i.key(stamp).Bytes()) {
		return false
	}

	i.appendValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToView()

	return true
}

// SeekGE implements Iterator.
func (i *unaryIterator) SeekGE(stamp telem.TimeStamp) bool {
	i.resetError()
	i.resetForceInternalValid()

	i.resetValue()

	if !i.internal.SeekGE(i.key(stamp).Bytes()) {
		return false
	}

	i.appendValue()
	i.updateView(i.value.UnboundedRange().Start.SpanRange(0))
	i.boundValueToView()

	return true
}

// Seek implements Iterator.
func (i *unaryIterator) Seek(stamp telem.TimeStamp) bool {

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

// Value implements Iterator.
func (i *unaryIterator) Value() *segment.Range { return i.value }

// Values implements Iterator.
func (i *unaryIterator) Values() []*segment.Range { return []*segment.Range{i.value} }

func (i *unaryIterator) GetValue(key channel.Key) *segment.Range {
	if key != 0 && key != i.channel.Key {
		return nil
	}
	return i.Value()
}

// View implements Iterator.
func (i *unaryIterator) View() telem.TimeRange { return i.view }

// Error implements Iterator.
func (i *unaryIterator) Error() error {
	if i.error() != nil {
		return i.error()
	}
	return i.internal.Error()
}

// Valid implements Iterator.
func (i *unaryIterator) Valid() bool {
	if i.error() != nil || i.View().IsZero() {
		return false
	}
	if i._forceInternalValid {
		return true
	}
	return i.internal.Valid()
}

// Close closes the iterator.
func (i *unaryIterator) Close() error {
	i.resetValue()
	return i.internal.Close()
}

func (i *unaryIterator) key(stamp telem.TimeStamp) segment.Key {
	return segment.NewKey(i.channel.Key, stamp)
}

func (i *unaryIterator) updateView(rng telem.TimeRange) { i.view = rng.BoundBy(i.rng) }

func (i *unaryIterator) appendValue() { i.value.Headers = append(i.value.Headers, i.internal.Value()) }

func (i *unaryIterator) prependValue() {
	i.value.Headers = append([]segment.Header{i.internal.Value()}, i.value.Headers...)
}

func (i *unaryIterator) boundValueToView() { i.value.Bound = i.View() }

func (i *unaryIterator) resetValue() {
	i.value = new(segment.Range)
	i.value.Bound = telem.TimeRangeMax
	i.value.Channel = i.channel
}

func (i *unaryIterator) forceInternalValid() { i._forceInternalValid = true }

func (i *unaryIterator) resetForceInternalValid() { i._forceInternalValid = false }

func (i *unaryIterator) setError(err error) {
	if err != nil {
		i._error = err
	}
}

func (i *unaryIterator) resetError() { i._error = nil }

func (i *unaryIterator) error() error { return i._error }

type compoundIterator []unaryIterator

// First implements Iterator.
func (i compoundIterator) First() bool {
	for _, it := range i {
		if !it.First() {
			return false
		}
	}
	return true
}

// Last implements Iterator.
func (i compoundIterator) Last() bool {
	for _, it := range i {
		if !it.Last() {
			return false
		}
	}
	return true
}

// Next implements Iterator.
func (i compoundIterator) Next() bool {
	for _, it := range i {
		if !it.Next() {
			return false
		}
	}
	return true
}

// Prev implements Iterator.
func (i compoundIterator) Prev() bool {
	for _, it := range i {
		if !it.Prev() {
			return false
		}
	}
	return true
}

// NextSpan implements Iterator.
func (i compoundIterator) NextSpan(span telem.TimeSpan) bool {
	for _, it := range i {
		if !it.NextSpan(span) {
			return false
		}
	}
	return true
}

// PrevSpan implements Iterator.
func (i compoundIterator) PrevSpan(span telem.TimeSpan) bool {
	for _, it := range i {
		if !it.PrevSpan(span) {
			return false
		}
	}
	return true
}

// NextRange implements Iterator.
func (i compoundIterator) NextRange(rng telem.TimeRange) bool {
	for _, it := range i {
		if !it.NextRange(rng) {
			return false
		}
	}
	return true
}

// SeekFirst implements Iterator.
func (i compoundIterator) SeekFirst() bool {
	for _, it := range i {
		if !it.SeekFirst() {
			return false
		}
	}
	return true
}

// SeekLast implements Iterator.
func (i compoundIterator) SeekLast() bool {
	for _, it := range i {
		if !it.SeekLast() {
			return false
		}
	}
	return true
}

// SeekLT implements Iterator.
func (i compoundIterator) SeekLT(stamp telem.TimeStamp) bool {
	for _, it := range i {
		if !it.SeekLT(stamp) {
			return false
		}
	}
	return true
}

// SeekGE implements Iterator.
func (i compoundIterator) SeekGE(stamp telem.TimeStamp) bool {
	for _, it := range i {
		if !it.SeekGE(stamp) {
			return false
		}
	}
	return true
}

// Seek implements Iterator.
func (i compoundIterator) Seek(stamp telem.TimeStamp) bool {
	for _, it := range i {
		if !it.Seek(stamp) {
			return false
		}
	}
	return true
}

// Value implements Iterator.
func (i compoundIterator) Value() *segment.Range { return i[0].Value() }

// Values implements Iterator.
func (i compoundIterator) Values() []*segment.Range {
	var values []*segment.Range
	for _, it := range i {
		values = append(values, it.Value())
	}
	return values
}

// GetValue implements Iterator.
func (i compoundIterator) GetValue(key channel.Key) *segment.Range {
	for _, it := range i {
		if it.channel.Key == key {
			return it.Value()
		}
	}
	return nil
}

// View implements Iterator.
func (i compoundIterator) View() telem.TimeRange { return i[0].View() }

// Error implements Iterator.
func (i compoundIterator) Error() error {
	for _, it := range i {
		if err := it.Error(); err != nil {
			return err
		}
	}
	return nil
}

// Valid implements Iterator.
func (i compoundIterator) Valid() bool {
	for _, it := range i {
		if !it.Valid() {
			return false
		}
	}
	return true
}

// Close implements Iterator.
func (i compoundIterator) Close() error {
	for _, it := range i {
		if err := it.Close(); err != nil {
			return err
		}
	}
	return nil
}
