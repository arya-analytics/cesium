package alamos

type Experiment interface {
	Sub(string) Experiment
	AddMeasurement(expMeasurement)
	Measurements() map[string]expMeasurement
}

type ExperimentWriter struct {
	experiment Experiment
}

type experiment struct {
	key          string
	children     map[string]Experiment
	measurements map[string]expMeasurement
}

func (e *experiment) Sub(key string) Experiment {
	exp := New(key)
	e.addSub(key, exp)
	return exp
}

func (e *experiment) addSub(key string, exp Experiment) Experiment {
	e.children[key] = exp
	return exp
}

func (e *experiment) AddMeasurement(m expMeasurement) {
	e.measurements[m.Key()] = m
}

func (e *experiment) Measurements() map[string]expMeasurement {
	return e.measurements
}

func New(name string) Experiment {
	return &experiment{
		key:          name,
		children:     make(map[string]Experiment),
		measurements: make(map[string]expMeasurement),
	}
}
