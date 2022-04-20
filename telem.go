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

const (
	Microsecond = TimeSpan(1)
	Millisecond = TimeSpan(1000 * Microsecond)
	Second      = TimeSpan(1000 * Millisecond)
	Minute      = TimeSpan(60 * Second)
	Hour        = TimeSpan(60 * Minute)
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

const secondsToMicroseconds = 1000000

func (dr DataRate) Period() TimeSpan {
	return TimeSpan(1 / float64(dr) * secondsToMicroseconds)
}

const (
	Hz1   DataRate = 1
	Hz10  DataRate = 10
	Hz5   DataRate = 5
	Hz100 DataRate = 100
)

// |||||| DENSITY ||||||

type DataType uint16

const (
	Float64 DataType = 8
)
