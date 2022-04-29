package alamos

import (
	"time"
)

// |||||| INTERFACE ||||||

// Duration is a metric that measures the execution time of a set of instructions. Values can be recorded either
// through the Record method or by creating a new Stopwatch and calling its Start and Stop methods.
// Duration is go-routine safe.
type Duration interface {
	Metric[time.Duration]
	// Stopwatch returns a new go-routine safe Stopwatch.
	Stopwatch() Stopwatch
}

// |||||| STOPWATCH ||||||

// Stopwatch is used to measure the execution time of a set of instructions.
type Stopwatch interface {
	// Start starts the stopwatch. Start should not be called more than once.
	Start()
	// Stop stops the stopwatch, binds the duration to the parent metric (Duration), and returns the duration.
	// Stop should not be called more than once, and will panic if called before start.
	Stop() time.Duration
	// Elapsed returns the time elapsed since Start was called.
	Elapsed() time.Duration
}

type stopwatch struct {
	metric Duration
	start  time.Time
}

// Start implement Stopwatch.
func (s *stopwatch) Start() {
	if !s.start.IsZero() {
		panic("duration defaultBaseMetric already started. please call Stop() first")
	}
	s.start = time.Now()
}

// Stop implement Stopwatch.
func (s *stopwatch) Stop() time.Duration {
	t := s.Elapsed()
	s.start = time.Time{}
	s.metric.Record(t)
	return t
}

// Elapsed implement Stopwatch.
func (s *stopwatch) Elapsed() time.Duration {
	if s.start.IsZero() {
		panic("duration defaultBaseMetric not started. please call Start() first")
	}
	return time.Since(s.start)
}

type emptyStopwatch struct{}

// Start implement Stopwatch.
func (s emptyStopwatch) Start() {}

// Stop implement Stopwatch.
func (s emptyStopwatch) Stop() time.Duration {
	return 0
}

// Elapsed implement Stopwatch.
func (s emptyStopwatch) Elapsed() time.Duration {
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

// NewSeriesDuration returns a new Duration metric that records all duration values in a Series.
func NewSeriesDuration(exp Experiment, key string) Duration {
	if m := nilDurationMeasurement(exp, key); m != nil {
		return m
	}
	return &duration{Metric: NewSeries[time.Duration](exp, key)}
}

// NewGaugeDuration returns a new Duration metric that records all duration values in a Gauge.
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

func (e emptyDurationMeasurement) Values() []time.Duration {
	return []time.Duration{}
}

func nilDurationMeasurement(exp Experiment, key string) Duration {
	if exp != nil {
		return nil
	}
	return emptyDurationMeasurement{Metric: emptyMetric[time.Duration](exp, key)}
}
