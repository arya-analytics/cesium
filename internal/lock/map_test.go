package lock_test

import (
	"cesium/internal/lock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Map", func() {
	It("Should allow the caller to acquire the lock", func() {
		m := lock.NewMap[int]()
		Expect(m.Acquire(1)).To(Succeed())
	})
	It("Should return an error when the caller tries to acquire a lock that is already held", func() {
		m := lock.NewMap[int]()
		Expect(m.Acquire(1)).To(Succeed())
		Expect(m.Acquire(1)).To(MatchError("lock already held"))
	})
	It("Should allow the called to release the lock", func() {
		m := lock.NewMap[int]()
		Expect(m.Acquire(1)).To(Succeed())
		m.Release(1)
		Expect(m.Acquire(1)).To(Succeed())
	})
})
