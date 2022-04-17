package cesium_test

import (
	"cesium"
	"cesium/util/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Create", func() {
	var (
		db  cesium.DB
		cpk cesium.PK
	)
	BeforeEach(func() {
		var err error
		db, err = cesium.Open("testdata")
		Expect(err).ToNot(HaveOccurred())
		c, err := db.NewCreateChannel().
			WithRate(cesium.Hz1).
			WithType(cesium.Float64).
			Exec(ctx)
		Expect(err).ToNot(HaveOccurred())
		cpk = c.PK
	})
	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
	})
	It("It should write the segment correctly", func() {
		req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
		Expect(err).ToNot(HaveOccurred())
		go func() {
			defer GinkgoRecover()
			Expect((<-res).Err).ToNot(HaveOccurred())
		}()
		ts := cesium.TimeStamp(0)
		for i := 0; i < 10; i++ {
			req <- cesium.CreateRequest{
				Segments: []cesium.Segment{
					{
						Start: ts,
						Data:  testutil.RandomFloat64Segment(100),
					},
					{
						Start: ts.Add(100 * cesium.Second),
						Data:  testutil.RandomFloat64Segment(100),
					},
				},
			}
		}
		time.Sleep(10 * time.Millisecond)
	})
})
