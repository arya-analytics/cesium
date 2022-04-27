package alamos

type Experiment interface {
	Sub(string) Experiment
	AddMeasurement(entry)
	Measurements() map[string]entry
}

type ExperimentWriter struct {
	experiment Experiment
}

type experiment struct {
	key          string
	children     map[string]Experiment
	measurements map[string]entry
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

func (e *experiment) AddMeasurement(m entry) {
	e.measurements[m.key()] = m
}

func (e *experiment) Measurements() map[string]entry {
	return e.measurements
}

func New(name string) Experiment {
	return &experiment{
		key:          name,
		children:     make(map[string]Experiment),
		measurements: make(map[string]entry),
	}
}

func SubExperiment(exp Experiment, key string) Experiment {
	if exp == nil {
		return nil
	}
	return exp.Sub(key)
}

type entry interface {
	key() string
}

func newEntry(key string) entry {
	return &baseEntry{k: key}
}

type baseEntry struct {
	k string
}

func (b *baseEntry) key() string {
	return b.k
}
