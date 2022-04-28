package alamos

import (
	"time"
)

// |||||| INTERFACE ||||||

type Duration interface {
	Metric[time.Duration]
	Record(time.Duration)
	Stopwatch() Stopwatch
}

// |||||| STOPWATCH ||||||

type Stopwatch interface {
	Start()
	Stop() time.Duration
}

type stopwatch struct {
	metric Duration
	start  time.Time
}

func (s *stopwatch) Start() {
	if !s.start.IsZero() {
		panic("duration entry already started. please call Stop() first")
	}
	s.start = time.Now()
}

func (s *stopwatch) Stop() time.Duration {
	if s.start.IsZero() {
		panic("duration entry not started. please call Start() first")
	}
	t := time.Since(s.start)
	s.metric.Record(t)
	return t
}

type emptyStopwatch struct{}

func (s *emptyStopwatch) Start() {}

func (s *emptyStopwatch) Stop() time.Duration {
	return 0
}

// |||||| BASE ||||||

type duration struct {
	start time.Time
	Metric[time.Duration]
}

func (d *duration) Stopwatch() Stopwatch {
	return &stopwatch{metric: d}
}

func NewSeriesDuration(exp Experiment, key string) Duration {
	if m := nilDurationMeasurement(exp, key); m != nil {
		return m
	}
	return &duration{Metric: NewSeries[time.Duration](exp, key)}
}

func NewGaugeDuration(exp Experiment, key string) Duration {
	if m := nilDurationMeasurement(exp, key); m != nil {
		return m
	}
	return &duration{Metric: NewGauge[time.Duration](exp, key)}
}

// |||||| EMPTY ||||||

type emptyDurationMeasurement struct {
	Metric[time.Duration]
}

func (e emptyDurationMeasurement) Record(time.Duration) {}

func (e emptyDurationMeasurement) Start() {}

func (e emptyDurationMeasurement) Stop() time.Duration {
	return 0
}

func (e emptyDurationMeasurement) Stopwatch() Stopwatch {
	return &emptyStopwatch{}
}

func nilDurationMeasurement(exp Experiment, key string) Duration {
	if exp != nil {
		return nil
	}
	return &emptyDurationMeasurement{nilMetric[time.Duration](exp, key)}
}
