package telem

type TimeStamp uint64

func (s TimeStamp) After(t TimeStamp) bool {
	return s > t
}

func (s TimeStamp) Before(t TimeStamp) bool {
	return s < t
}

type TimeRange struct {
	Start TimeStamp
	End   TimeStamp
}

type Size uint64

type DataRate float64

type Density uint16
