package lock_test

import (
	"cesium/internal/lock"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Lock", func() {
	It("Should allow the caller to a acquire the lock", func() {
		l := lock.New()
		l.Acquire()
		Expect(l.TryAcquire()).To(BeFalse())
	})
	It("Should allow the caller to release the lock", func() {
		l := lock.New()
		l.Acquire()
		l.Release()
		Expect(l.TryAcquire()).To(BeTrue())
	})
	Specify("Lock release should be idempotent", func() {
		l := lock.New()
		l.Acquire()
		l.Release()
		l.Release()
		Expect(l.TryAcquire()).To(BeTrue())
	})
})
