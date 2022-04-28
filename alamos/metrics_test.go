package alamos_test

import (
	"cesium/alamos"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Metric", func() {
	var (
		exp alamos.Experiment
	)
	BeforeEach(func() {
		exp = alamos.New("test")
	})
	Describe("Series", func() {
		It("Should create a Series entry", func() {
			Expect(func() {
				alamos.NewSeries[int8](exp, "test.series")
			}).ToNot(Panic())
		})
		It("Should show up in the list of measurements", func() {
			alamos.NewSeries[int8](exp, "test.series")
			_, ok := exp.Metrics()["test.series"]
			Expect(ok).To(BeTrue())
		})
		It("Should record values to the series", func() {
			series := alamos.NewSeries[float64](exp, "test.series")
			series.Record(1.0)
			Expect(series.Values()).To(Equal([]float64{1}))
		})
	})
	Describe("Gauge", func() {
		It("Should create a Gauge entry", func() {
			Expect(func() { alamos.NewGauge[int8](exp, "test.gauge") }).ToNot(Panic())
		})
		It("Should set the value on teh gauge", func() {
			gauge := alamos.NewGauge[float64](exp, "test.gauge")
			gauge.Record(1)
			Expect(gauge.Values()[0]).To(Equal(1.0))
		})
	})
})
