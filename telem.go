package caesium

// |||||| TIME STAMP ||||||

type TimeStamp int64

func (s TimeStamp) After(t TimeStamp) bool {
	return s > t
}

func (s TimeStamp) Before(t TimeStamp) bool {
	return s < t
}

func (s TimeStamp) Add(tspan TimeSpan) TimeStamp {
	return TimeStamp(int64(s) + int64(tspan))
}

// |||||| TIME RANGE ||||||

type TimeRange struct {
	Start TimeStamp
	End   TimeStamp
}

// |||||| TIME SPAN ||||||

type TimeSpan int64

const (
	Microsecond = TimeSpan(1)
	Millisecond = TimeSpan(1000 * Microsecond)
	Second      = TimeSpan(1000 * Millisecond)
	Minute      = TimeSpan(60 * Second)
	Hour        = TimeSpan(60 * Minute)
)

// |||||| SIZE ||||||

type Size uint64

// |||||| DATA RATE ||||||

type DataRate float64

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
