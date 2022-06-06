package cesium_test

import (
	"github.com/arya-analytics/cesium"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CreateChannel", func() {
	var db cesium.DB
	BeforeEach(func() {
		var err error
		db, err = cesium.Open("testdata")
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
	})
	It("Should create the Channel correctly", func() {
		c, err := db.NewCreateChannel().
			WithRate(cesium.DataRate(25)).
			WithType(cesium.Density(8)).
			Exec(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(c.DataRate).To(Equal(cesium.DataRate(25)))
		Expect(c.DataType).To(Equal(cesium.Density(8)))
	})
	Specify("The Channel can be retrieved after creation", func() {
		c, err := db.NewCreateChannel().
			WithRate(cesium.DataRate(25)).
			WithType(cesium.Density(8)).
			Exec(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(c.DataRate).To(Equal(cesium.DataRate(25)))
		Expect(c.DataType).To(Equal(cesium.Density(8)))
		resC, err := db.NewRetrieveChannel().WhereKey(c.Key).Exec(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(resC.DataRate).To(Equal(cesium.DataRate(25)))
		Expect(resC.DataType).To(Equal(cesium.Density(8)))
	})
})
