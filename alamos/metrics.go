package alamos

import "time"

type Metric[T any] interface {
	Record(T)
	Values() []T
	Count() int
}

// |||||| GAUGE ||||||

type gauge[T Numeric] struct {
	entry
	count int
	value T
}

func NewGauge[T Numeric](exp Experiment, key string) Metric[T] {
	m := &gauge[T]{entry: newEntry(key)}
	exp.AddMeasurement(m)
	return m
}

func (g *gauge[T]) Count() int {
	return g.count
}

func (g *gauge[T]) Values() []T {
	return []T{g.value / T(g.count)}
}

func (g *gauge[T]) Record(v T) {
	g.count++
	g.value = v
}

// |||||| SERIES ||||||

type series[T any] struct {
	entry
	values []T
}

func (s *series[T]) Value() interface{} {
	return s.values
}

func (s *series[T]) Values() []T {
	return s.values
}

func (s *series[T]) Record(v T) {
	s.values = append(s.values, v)
}

func (s *series[T]) Count() int {
	return len(s.values)
}

func NewSeries[T any](exp Experiment, key string) Metric[T] {
	if m := nilMetric[T](exp, key); m != nil {
		return m
	}
	m := &series[T]{entry: newEntry(key)}
	exp.AddMeasurement(m)
	return m
}

// |||||| EMPTY ||||||

type empty[T any] struct {
	entry
}

func (e *empty[T]) Value() interface{} {
	return nil
}

func (e *empty[T]) Values() []T {
	return nil
}

func (e *empty[T]) Record(T) {}

func (e *empty[T]) Count() int {
	return 0
}

func nilMetric[T any](exp Experiment, key string) Metric[T] {
	if exp != nil {
		return nil
	}
	m := &empty[T]{entry: newEntry(key)}
	exp.AddMeasurement(m)
	return m
}

type Numeric interface {
	float64 | float32 | int64 | int32 | int16 | int8 | uint64 | uint32 | uint16 | uint8 | time.Duration
}
