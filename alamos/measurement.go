package alamos

import "time"

type expMeasurement interface {
	Key() string
	Value() interface{}
}

type Measurement[T any] interface {
	Record(T)
	Values() []T
}

type baseMeasurement struct {
	key string
}

func (b *baseMeasurement) Key() string {
	return b.key
}

// |||||| GAUGE ||||||

type GaugeMeasurement[T any] struct {
	baseMeasurement
	value T
}

func NewGauge[T any](exp Experiment, key string) Measurement[T] {
	m := &GaugeMeasurement[T]{baseMeasurement: baseMeasurement{key: key}}
	exp.AddMeasurement(m)
	return m
}

func (g *GaugeMeasurement[T]) Name() string {
	return g.key
}

func (g *GaugeMeasurement[T]) Value() interface{} {
	return g.value
}

func (g *GaugeMeasurement[T]) Values() []T {
	return []T{g.value}
}

func (g *GaugeMeasurement[T]) Record(v T) {
	g.value = v
}

// |||||| SERIES ||||||

type SeriesMeasurement[T any] struct {
	baseMeasurement
	values []T
}

func (s *SeriesMeasurement[T]) Value() interface{} {
	return s.values
}

func (s *SeriesMeasurement[T]) Values() []T {
	return s.values
}

func (s *SeriesMeasurement[T]) Record(v T) {
	s.values = append(s.values, v)
}

func NewSeries[T any](exp Experiment, key string) Measurement[T] {
	if m := nilMeasurement[T](exp, key); m != nil {
		return m
	}
	m := &SeriesMeasurement[T]{baseMeasurement: baseMeasurement{key: key}, values: []T{}}
	exp.AddMeasurement(m)
	return m
}

// |||||| EMPTY ||||||

type EmptyMeasurement[T any] struct {
	baseMeasurement
}

func (e *EmptyMeasurement[T]) Value() interface{} {
	return nil
}

func (e *EmptyMeasurement[T]) Values() []T {
	return nil
}

func (e *EmptyMeasurement[T]) Record(T) {}

func nilMeasurement[T any](exp Experiment, key string) Measurement[T] {
	if exp != nil {
		return nil
	}
	m := &EmptyMeasurement[T]{baseMeasurement: baseMeasurement{key: key}}
	exp.AddMeasurement(m)
	return m
}

// |||||| DURATION ||||||

type DurationMeasurement interface {
	Measurement[time.Duration]
	Record(time.Duration)
	Start()
	Stop()
}

type durationSeriesMeasurement struct {
	baseMeasurement
	ts     time.Time
	values []time.Duration
}

func (d *durationSeriesMeasurement) Record(v time.Duration) {
	d.values = append(d.values, v)
}

func (d *durationSeriesMeasurement) Value() interface{} {
	return d.values
}

func (d *durationSeriesMeasurement) Values() []time.Duration {
	return d.values
}

func (d *durationSeriesMeasurement) Start() {
	if !d.ts.IsZero() {
		panic("duration measurement already started. please call Stop() first")
	}
	d.ts = time.Now()
}

func (d *durationSeriesMeasurement) Stop() {
	if d.ts.IsZero() {
		panic("duration measurement not started. please call Start() first")
	}
	d.values = append(d.values, time.Now().Sub(d.ts))
	d.ts = time.Time{}
}

type emptyDurationMeasurement struct {
	baseMeasurement
}

func (e emptyDurationMeasurement) Record(time.Duration) {}

func (e emptyDurationMeasurement) Start() {}

func (e emptyDurationMeasurement) Stop() {}

func (e emptyDurationMeasurement) Value() interface{} {
	return nil
}

func (e emptyDurationMeasurement) Values() []time.Duration {
	return nil
}

func nilDurationMeasurement(exp Experiment, key string) DurationMeasurement {
	if exp != nil {
		return nil
	}
	return &emptyDurationMeasurement{baseMeasurement: baseMeasurement{key: key}}
}

func NewDurationSeries(exp Experiment, key string) DurationMeasurement {
	if m := nilDurationMeasurement(exp, key); m != nil {
		return m
	}
	m := &durationSeriesMeasurement{baseMeasurement: baseMeasurement{key: key}}
	exp.AddMeasurement(m)
	return m
}
