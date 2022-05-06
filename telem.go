package cesium

import (
	"strconv"
	"time"
)

// |||||| TIME STAMP ||||||

const (
	// TimeStampMin represents the minimum value for a TimeStamp
	TimeStampMin = TimeStamp(0)
	// TimeStampMax represents the maximum value for a TimeStamp
	TimeStampMax = TimeStamp(^uint64(0) >> 1)
)

// TimeStamp stores an epoch time in nanoseconds.
type TimeStamp int64

// Now returns the current time as a TimeStamp.
func Now() TimeStamp {
	return NewTimeStamp(time.Now())
}

// NewTimeStamp creates a new TimeStamp from a time.Time.
func NewTimeStamp(t time.Time) TimeStamp {
	return TimeStamp(t.UnixNano())
}

// Time returns the time.Time representation of the TimeStamp.
func (ts TimeStamp) Time() time.Time {
	return time.Unix(0, int64(ts))
}

// IsZero returns true if the TimeStamp is TimeStampMin.
func (ts TimeStamp) IsZero() bool {
	return ts == TimeStampMin
}

// After returns true if the TimeStamp is greater than the provided one.
func (ts TimeStamp) After(t TimeStamp) bool {
	return ts > t
}

// Before returns true if the TimeStamp is less than the provided one.
func (ts TimeStamp) Before(t TimeStamp) bool {
	return ts < t
}

// Add returns a new TimeStamp with the provided TimeSpan added to it.
func (ts TimeStamp) Add(tspan TimeSpan) TimeStamp {
	return TimeStamp(int64(ts) + int64(tspan))
}

// SpanRange constructs a new TimeRange with the TimeStamp and provided TimeSpan.
func (ts TimeStamp) SpanRange(span TimeSpan) TimeRange {
	return ts.Range(ts.Add(span))
}

// Range constructs a new TimeRange with the TimeStamp and provided TimeStamp.
func (ts TimeStamp) Range(ts2 TimeStamp) TimeRange {
	return TimeRange{ts, ts2}
}

func (ts TimeStamp) String() string {
	return ts.Time().String()
}

// |||||| TIME RANGE ||||||

// TimeRange represents a range of time between two TimeStamp.
type TimeRange struct {
	// Start is the start of the range.
	Start TimeStamp
	// End is the end of the range.
	End TimeStamp
}

// Span returns the TimeSpan that the TimeRange occupies.
func (tr TimeRange) Span() TimeSpan {
	return TimeSpan(tr.End - tr.Start)
}

// IsZero returns true if the TimeSpan of TimeRange is empty.
func (tr TimeRange) IsZero() bool {
	return tr.Span().IsZero()
}

var (
	TimeRangeMax = TimeRange{Start: TimeStampMin, End: TimeStampMax}
)

// |||||| TIME SPAN ||||||

type TimeSpan int64

const (
	TimeSpanMin = TimeSpan(0)
	TimeSpanMax = TimeSpan(^uint64(0) >> 1)
)

// Duration converts TimeSpan to a values.Duration.
func (ts TimeSpan) Duration() time.Duration {
	return time.Duration(ts) * time.Microsecond
}

func (ts TimeSpan) Seconds() float64 {
	return float64(ts) / float64(Second)
}

func (ts TimeSpan) IsZero() bool {
	return ts == TimeSpanMin
}

const (
	Microsecond = TimeSpan(1)
	Millisecond = 1000 * Microsecond
	Second      = 1000 * Millisecond
	Minute      = 60 * Second
	Hour        = 60 * Minute
)

// |||||| SIZE ||||||

type Size int64

const (
	Kilobytes Size = 1024
)

func (s Size) String() string {
	return strconv.FormatInt(int64(s), 10)
}

// |||||| DATA RATE ||||||

type DataRate float64

func (dr DataRate) Period() TimeSpan {
	return TimeSpan(1 / float64(dr) * float64(Second))
}

func (dr DataRate) SampleCount(t TimeSpan) int {
	return int(t.Seconds() * float64(dr))
}

func (dr DataRate) Span(sampleCount int) TimeSpan {
	return dr.Period() * TimeSpan(sampleCount)
}

func (dr DataRate) ByteSpan(byteCount int, dataType DataType) TimeSpan {
	return dr.Span(byteCount / int(dataType))
}

const (
	Hz DataRate = 1
)

// |||||| DENSITY ||||||

type DataType uint16

const (
	Float64 DataType = 8
)
