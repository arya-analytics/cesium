package alamos

import (
	"time"
)

// |||||| INTERFACE ||||||

type Duration interface {
	Metric[time.Duration]
	Record(time.Duration)
	Start()
	Stop() time.Duration
}

// |||||| BASE ||||||

type duration struct {
	start time.Time
	Metric[time.Duration]
}

func (b *duration) Start() {
	if !b.start.IsZero() {
		panic("duration entry already started. please call Stop() first")
	}
	b.start = time.Now()
}

func (b *duration) Stop() time.Duration {
	if b.start.IsZero() {
		panic("duration entry not started. please call Start() first")
	}
	defer func() {
		b.start = time.Time{}
	}()
	t := time.Since(b.start)
	b.Record(t)
	return t
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

func nilDurationMeasurement(exp Experiment, key string) Duration {
	if exp != nil {
		return nil
	}
	return &emptyDurationMeasurement{nilMetric[time.Duration](exp, key)}
}
