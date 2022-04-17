package caesium_test

import (
	"caesium/util/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	log "github.com/sirupsen/logrus"
	"io"
	"time"

	"caesium"
)

var _ = Describe("Retrieve", func() {
	var (
		db  caesium.DB
		cpk caesium.PK
	)
	BeforeEach(func() {
		var err error
		db, err = caesium.Open("testdata")
		Expect(err).ToNot(HaveOccurred())
		c, err := db.NewCreateChannel().
			WithRate(caesium.Hz1).
			WithType(caesium.Float64).
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
			sampleSpan := caesium.TimeSpan(nSamples) * caesium.Second
			ts := caesium.TimeStamp(0).Add(
				caesium.TimeSpan(i) * 2 * sampleSpan,
			)
			req <- caesium.CreateRequest{
				Segments: []caesium.Segment{
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
		rResV, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(caesium.TimeRange{
			Start: caesium.TimeStamp(0),
			End:   caesium.TimeStamp(200 * caesium.Second),
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
