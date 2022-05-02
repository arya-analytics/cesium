package kv_test

import (
	"github.com/arya-analytics/cesium/internal/kv"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("KVKey", func() {
	Describe("CompositeKey", func() {
		It("Should generate a composite key from elements", func() {
			key := kv.CompositeKey("foo", "bar")
			Expect(key).To(Equal([]byte("foobar")))
		})
	})
})
