package errutil_test

import (
	"errors"
	"github.com/arya-analytics/cesium/internal/errutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Convert", func() {
	var c errutil.ConvertChain
	BeforeEach(func() {
		c = errutil.ConvertChain{func(err error) (error, bool) {
			if err.Error() == "not random error" {
				return errors.New("random error"), true
			}
			return nil, false
		}}
	})
	It("Should convert an error", func() {
		Expect(c.Exec(errors.New("not random error"))).To(Equal(errors.New("random error")))
	})
	It("Should return nil if the submitted error is nil", func() {
		Expect(c.Exec(nil)).To(BeNil())
	})
	It("Should return the original error if it can't be converted", func() {
		Expect(c.Exec(errors.New("very very random error"))).To(Equal(errors.New("very very random error")))
	})
})
