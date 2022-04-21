package shut_test

import (
	"cesium/shut"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Shutter", func() {
	var (
		s shut.Shutter
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
	Describe("Close", func() {
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
			Expect(s.Close()).To(Succeed())
			Expect(exited).To(Equal([]bool{true, true}))
			Expect(s.NumRoutines()).To(Equal(0))
		})
	})
})
