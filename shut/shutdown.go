package shut

import (
	"sync"
	"time"
)

type Signal int

const DefaultShutdownThreshold = 10 * time.Second

type Shutdown interface {
	Go(f func(chan Signal) error, opts ...GoOption)
	Routines() map[string]int
	NumRoutines() int
	Shutdown() error
}

func New(opts ...Option) Shutdown {
	opt := newOptions(opts...)
	return &shutter{
		signal:   make(chan Signal),
		opts:     opt,
		routines: make(map[string]int),
		errors:   make(chan error, 10),
	}
}

type shutter struct {
	signal   chan Signal
	mu       sync.Mutex
	routines map[string]int
	count    int
	errors   chan error
	opts     *options
}

func (s *shutter) Go(f func(chan Signal) error, opts ...GoOption) {
	goOpt := newGoOpts(opts...)
	go func() {
		err := f(s.signal)
		s.closeRoutine(goOpt.key, err)
	}()
	s.addRoutine(goOpt.key)
}

func (s *shutter) Shutdown() error {
	close(s.signal)
	t := time.NewTimer(s.opts.shutdownThreshold)
	var errors []error
o:
	for {
		select {
		case err := <-s.errors:
			errors = append(errors, err)
			if len(errors) >= s.count {
				break o
			}
		case <-t.C:
			panic("[shutter.Shutdown] graceful shutdown timeout exceeded")
		}
	}
	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *shutter) Routines() map[string]int {
	nRoutines := make(map[string]int)
	s.mu.Lock()
	for k, v := range s.routines {
		nRoutines[k] = v
	}
	return nRoutines
}

func (s *shutter) NumRoutines() int {
	return s.count
}

const defaultRoutineKey = "unkeyed"

func (s *shutter) addRoutine(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if key == "" {
		key = defaultRoutineKey
	}
	s.count++
	s.routines[key]++
}

func (s *shutter) closeRoutine(key string, err error) {
	if key == "" {
		key = defaultRoutineKey
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.routines[key]--
	s.count--
	if s.routines[key] == 0 {
		delete(s.routines, key)
	}
	s.errors <- err
}

// |||||| OPTIONS ||||||

type options struct {
	shutdownThreshold time.Duration
}

type Option func(*options)

func newOptions(opts ...Option) *options {
	o := &options{
		shutdownThreshold: DefaultShutdownThreshold,
	}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithThreshold(threshold time.Duration) Option {
	return func(o *options) {
		o.shutdownThreshold = threshold
	}
}

// |||||| GO OPTIONS ||||||

type goOptions struct {
	key string
}

func newGoOpts(opts ...GoOption) goOptions {
	var goOpt goOptions
	for _, o := range opts {
		o(&goOpt)
	}
	return goOpt
}

type GoOption func(*goOptions)

func WithKey(key string) GoOption {
	return func(goOpt *goOptions) {
		goOpt.key = key
	}
}
