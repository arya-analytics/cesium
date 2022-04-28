package alamos_test

import (
	"cesium/alamos"
	"encoding/json"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
)

var _ = Describe("Report", func() {
	FIt("Should write the experiment to JSON", func() {
		exp := alamos.New("exp")
		g := alamos.NewGauge[int](exp, "gauge")
		g.Record(1)
		g2 := alamos.NewGauge[int](exp, "gauge2")
		g2.Record(2)
		log.Info(exp)
		sub := alamos.Sub(exp, "sub")
		g3 := alamos.NewSeries[float64](sub, "gauge3")
		g3.Record(3.2)
		file, _ := json.Marshal(exp.Report())
		err := ioutil.WriteFile("report.json", file, 0644)
		Expect(err).To(BeNil())
	})
})
