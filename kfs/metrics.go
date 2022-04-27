package kfs

import "cesium/alamos"

type Metrics struct {
	// Acquire tracks the number of times a File is acquired, and the average time to acquire a File.
	Acquire alamos.Duration
	// Release tracks the number of times a File is released, and the average time to release a File.
	Release alamos.Duration
	// Delete tracks the number of files deleted, and the average time to delete a file.
	Delete alamos.Duration
}

func newMetrics(exp alamos.Experiment) Metrics {
	subExp := alamos.SubExperiment(exp, "kfs.FS")
	return Metrics{
		Acquire: alamos.NewGaugeDuration(subExp, "Acquire"),
		Release: alamos.NewGaugeDuration(subExp, "Release"),
		Delete:  alamos.NewGaugeDuration(subExp, "Delete"),
	}
}
