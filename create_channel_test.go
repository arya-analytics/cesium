package caesium_test

import (
	"caesium"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("CreateChannel", func() {
	var db caesium.DB
	BeforeEach(func() {
		var err error
		db, err = caesium.New("testdata")
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
	})
	It("Should create the channel correctly", func() {
		c, err := db.NewCreateChannel().
			WithDataRate(caesium.DataRate(25)).
			WithDensity(caesium.Density(8)).
			Exec(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(c.DataRate).To(Equal(caesium.DataRate(25)))
		Expect(c.Density).To(Equal(caesium.Density(8)))
	})
	Specify("The channel can be retrieved after creation", func() {
		c, err := db.NewCreateChannel().
			WithDataRate(caesium.DataRate(25)).
			WithDensity(caesium.Density(8)).
			Exec(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(c.DataRate).To(Equal(caesium.DataRate(25)))
		Expect(c.Density).To(Equal(caesium.Density(8)))
		resC, err := db.NewRetrieveChannel().WherePK(c.PK).Exec(ctx)
		Expect(err).ToNot(HaveOccurred())
		Expect(resC.DataRate).To(Equal(caesium.DataRate(25)))
		Expect(resC.Density).To(Equal(caesium.Density(8)))
	})
})
