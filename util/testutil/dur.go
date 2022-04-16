package testutil

import (
	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega/gmeasure"
	"time"
)

func RunDurationExp(name string, n int, f func()) {
	exp := gmeasure.NewExperiment(name)
	ginkgo.AddReportEntry(exp.Name, exp)
	exp.Sample(func(idx int) {
		exp.MeasureDuration(name, f, gmeasure.Precision(1*time.Microsecond))
	}, gmeasure.SamplingConfig{N: n})
}
