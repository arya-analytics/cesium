package kfs

import "cesium/alamos"

type options struct {
	baseFS     BaseFS
	suffix     string
	experiment alamos.Experiment
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

func mergeDefaults(o *options) {
	if o.suffix == "" {
		o.suffix = ".kfs"
	}
	if o.baseFS == nil {
		o.baseFS = &osFS{}
	}
}

func WithFS(fs BaseFS) Option {
	return func(o *options) {
		o.baseFS = fs
	}
}

func WithExperiment(e alamos.Experiment) Option {
	return func(o *options) {
		o.experiment = e
	}
}

func WithSuffix(s string) Option {
	return func(o *options) {
		o.suffix = s
	}
}
