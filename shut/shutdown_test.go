package shut_test

import (
	"github.com/arya-analytics/cesium/shut"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Shutdown", func() {
	var (
		s shut.Shutdown
	)
	BeforeEach(func() {
		s = shut.New()
	})
	Describe("Routines", func() {
		It("Should start a new routine correctly", func() {
			s.Go(func(sig chan shut.Signal) error {
				<-sig
				return nil
			}, shut.WithKey("routine"))
			Expect(s.Routines()["routine"]).To(Equal(1))
		})
	})
	Describe("Shutdown", func() {
		It("Should close all routines", func() {
			exited := make([]bool, 2)
			s.Go(func(sig chan shut.Signal) error {
				<-sig
				exited[0] = true
				return nil
			}, shut.WithKey("routine"))
			s.Go(func(sign chan shut.Signal) error {
				<-sign
				exited[1] = true
				return nil
			}, shut.WithKey("routine"))
			Expect(s.Shutdown()).To(Succeed())
			Expect(exited[0]).To(Equal(true))
			Expect(exited[1]).To(Equal(true))
			Expect(s.NumRoutines()).To(Equal(0))
		})
	})
})
