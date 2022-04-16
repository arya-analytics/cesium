package caesium

// |||||| TIME STAMP ||||||

type TimeStamp uint64

func (s TimeStamp) After(t TimeStamp) bool {
	return s > t
}

func (s TimeStamp) Before(t TimeStamp) bool {
	return s < t
}

// |||||| TIME RANGE ||||||

type TimeRange struct {
	Start TimeStamp
	End   TimeStamp
}

// |||||| SIZE ||||||

type Size uint64

// |||||| DATA RATE ||||||

type DataRate float64

// |||||| DENSITY ||||||

type Density uint16
