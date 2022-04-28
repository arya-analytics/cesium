package kfs

import (
	"cesium/alamos"
	"time"
)

type options struct {
	baseFS          BaseFS
	suffix          string
	experiment      alamos.Experiment
	maxSyncInterval time.Duration
}

type Option func(o *options)

func newOptions(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	mergeDefaults(o)
	return o
}

const defaultSuffix = ".kfs"

func mergeDefaults(o *options) {
	if o.suffix == "" {
		o.suffix = defaultSuffix
	}
	if o.baseFS == nil {
		o.baseFS = &osFS{}
	}
}

// WithFS sets the base filesystem to use.
func WithFS(fs BaseFS) Option {
	return func(o *options) {
		o.baseFS = fs
	}
}

// WithExperiment sets the experiment that the KFS uses to record its Metrics.
func WithExperiment(e alamos.Experiment) Option {
	return func(o *options) {
		o.experiment = e
	}
}

// WithSuffix sets the suffix that the KFS uses to append to its filenames.
func WithSuffix(s string) Option {
	return func(o *options) {
		o.suffix = s
	}
}
