package cesium_test

import (
	"cesium/util/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"io"
	"time"

	"cesium"
)

var _ = Describe("Retrieve", func() {
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
	FIt("Should read the segments correctly", func() {
		const nSamples = 500
		req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
		Expect(err).ToNot(HaveOccurred())
		go func() {
			defer GinkgoRecover()
			Expect((<-res).Err).ToNot(HaveOccurred())
		}()
		for i := 0; i < 10; i++ {
			sampleSpan := cesium.TimeSpan(nSamples) * cesium.Second
			ts := cesium.TimeStamp(0).Add(
				cesium.TimeSpan(i) * 2 * sampleSpan,
			)
			req <- cesium.CreateRequest{
				Segments: []cesium.Segment{
					{
						Start: ts,
						Data:  testutil.RandomFloat64Segment(nSamples),
					},
					{
						Start: ts.Add(sampleSpan),
						Data:  testutil.RandomFloat64Segment(nSamples),
					},
				},
			}
		}
		time.Sleep(10 * time.Millisecond)

		t0 := time.Now()
		rResV, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(cesium.TimeRange{
			Start: cesium.TimeStamp(0),
			End:   cesium.TimeStamp(200 * cesium.Second),
		}).Stream(ctx)
		Expect(err).ToNot(HaveOccurred())
		for resV := range rResV {
			if resV.Err == io.EOF {
				break
			}
			Expect(resV.Err).ToNot(HaveOccurred())
			Expect(len(resV.Segments[0].Data)).To(Equal(nSamples * 8))
		}
		log.Info(time.Since(t0))
	})
})
