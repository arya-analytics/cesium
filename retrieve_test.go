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
		cesium.
		const nSamples = 5000
		req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
		Expect(err).ToNot(HaveOccurred())
		sampleSpan := cesium.TimeSpan(nSamples) * cesium.Second
		segCount := 10000
		for i := 0; i < segCount/2; i++ {
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
		close(req)
		for cResV := range res {
			if cResV.Err == io.EOF {
				break
			}
			Expect(cResV.Err).ToNot(HaveOccurred())
		}

		t0 := time.Now()
		rResV, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(cesium.TimeRange{
			Start: cesium.TimeStamp(0),
			End:   cesium.TimeStamp(sampleSpan * cesium.TimeSpan(segCount)),
		}).Stream(ctx)
		Expect(err).ToNot(HaveOccurred())
		c := 0
		for resV := range rResV {
			if resV.Err == io.EOF {
				break
			}
			c++
			//b := resV.Segments[0].Data
			Expect(resV.Err).ToNot(HaveOccurred())
			//Expect(math.Float64frombits(binary.Encoding().Uint64(b[len(b)-8:]))).To(Equal(float64(10099)))
			Expect(len(resV.Segments[0].Data)).To(Equal(nSamples * 8))
		}
		//Expect(c).To(Equal(20))
		log.Infof(`Retrieved 
	Sample count: %d
	Time: %s
	Segment Count: %v,
	Throughput: %v samples/s`,
			c*nSamples, time.Since(t0), c, float64(c*nSamples)/time.Since(t0).Seconds())
		time.Sleep(5 * time.Millisecond)
	})
})
