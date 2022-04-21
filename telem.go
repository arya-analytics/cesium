package cesium

import (
	"cesium/util/binary"
	"strconv"
	"time"
)

// |||||| TIME STAMP ||||||

type TimeStamp int64

func (ts TimeStamp) String() string {
	return time.UnixMicro(int64(ts)).String()
}

func Now() TimeStamp {
	return NewTimeStamp(time.Now())
}

func NewTimeStamp(t time.Time) TimeStamp {
	return TimeStamp(t.UnixMicro())
}

var (
	TimeStampMin = TimeStamp(0)
	TimeStampMax = TimeStamp(^uint64(0) >> 1)
)

func (ts TimeStamp) Bytes() []byte {
	b := make([]byte, 8)
	binary.Encoding().PutUint64(b, uint64(ts))
	return b
}

func (ts TimeStamp) After(t TimeStamp) bool {
	return ts > t
}

func (ts TimeStamp) Before(t TimeStamp) bool {
	return ts < t
}

func (ts TimeStamp) Add(tspan TimeSpan) TimeStamp {
	return TimeStamp(int64(ts) + int64(tspan))
}

// |||||| TIME RANGE ||||||

type TimeRange struct {
	Start TimeStamp
	End   TimeStamp
}

func NewTimeRange(start, end TimeStamp) TimeRange {
	return TimeRange{Start: start, End: end}
}

func (tr TimeRange) Span() TimeSpan {
	return TimeSpan(tr.End - tr.Start)
}

func (tr TimeRange) IsZero() bool {
	return tr.Span() == 0
}

var (
	TimeRangeMax = TimeRange{Start: TimeStampMin, End: TimeStampMax}
)

// |||||| TIME SPAN ||||||

type TimeSpan int64

// Duration converts TimeSpan to a values.Duration.
func (ts TimeSpan) Duration() time.Duration {
	return time.Duration(ts) * time.Microsecond
}

func (ts TimeSpan) Seconds() float64 {
	return float64(ts) / float64(Second)
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
