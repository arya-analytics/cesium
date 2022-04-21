package cesium_test

import (
	"cesium"
	"cesium/util/testutil/seg"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

var _ = Describe("Retrieve", func() {
	Context("Single Channel", func() {
		var (
			db  cesium.DB
			cpk cesium.PK
			c   cesium.Channel
		)
		BeforeEach(func() {
			var err error
			db, err = cesium.Open("testdata", cesium.MemBacked())
			Expect(err).ToNot(HaveOccurred())
			c, err = db.NewCreateChannel().WithRate(cesium.Hz).WithType(cesium.Float64).Exec(ctx)
			Expect(err).ToNot(HaveOccurred())
			cpk = c.PK
		})
		AfterEach(func() {
			Expect(db.Close()).To(Succeed())
		})
		It("Should read the segments correctly", func() {
			req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
			stc := &seg.StreamCreate{
				Req:               req,
				Res:               res,
				SequentialFactory: seg.NewSequentialFactory(c, seg.RandFloat64, 10*cesium.Second),
			}
			Expect(err).ToNot(HaveOccurred())
			stc.CreateCRequestsOfN(10, 2)
			Expect(stc.CloseAndWait()).To(Succeed())
			rResV, err := db.NewRetrieve().
				WhereChannels(cpk).
				WhereTimeRange(cesium.TimeRangeMax).
				Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			segments, err := seg.StreamRetrieve{Res: rResV}.All()
			Expect(err).ToNot(HaveOccurred())
			Expect(segments).To(HaveLen(20))
		})
		It("It should support multiple concurrent read requests", func() {
			req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			stc := &seg.StreamCreate{
				Req:               req,
				Res:               res,
				SequentialFactory: seg.NewSequentialFactory(c, seg.RandFloat64, 10*cesium.Second),
			}
			stc.CreateCRequestsOfN(10, 2)
			Expect(stc.CloseAndWait()).To(Succeed())
			const nRequests = 10
			wg := sync.WaitGroup{}
			wg.Add(nRequests)
			for i := 0; i < nRequests; i++ {
				go func() {
					rResV, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(cesium.TimeRangeMax).Stream(ctx)
					Expect(err).ToNot(HaveOccurred())
					segments, err := seg.StreamRetrieve{Res: rResV}.All()
					Expect(err).ToNot(HaveOccurred())
					Expect(segments).To(HaveLen(20))
					wg.Done()
				}()
			}
			wg.Wait()
		})
	})
	Context("Multi Channel", func() {
		var (
			db           cesium.DB
			channels     []cesium.Channel
			channelCount = 10
		)
		BeforeEach(func() {
			var err error
			db, err = cesium.Open("testdata", cesium.MemBacked())
			Expect(err).ToNot(HaveOccurred())
			for i := 0; i < channelCount; i++ {
				c, err := db.NewCreateChannel().WithRate(1 * cesium.Hz).WithType(cesium.Float64).Exec(ctx)
				Expect(err).ToNot(HaveOccurred())
				channels = append(channels, c)
			}
		})
		AfterEach(func() {
			Expect(db.Close()).To(Succeed())
		})
		It("Should support reading data from multiple channels", func() {
			for i := 0; i < channelCount; i++ {
				req, res, err := db.NewCreate().WhereChannels(channels[i].PK).Stream(ctx)
				Expect(err).ToNot(HaveOccurred())
				stc := &seg.StreamCreate{
					Req:               req,
					Res:               res,
					SequentialFactory: seg.NewSequentialFactory(channels[i], seg.RandFloat64, 10*cesium.Second),
				}
				stc.CreateCRequestsOfN(10, 2)
				Expect(stc.CloseAndWait()).To(Succeed())
			}
			var cPKs []cesium.PK
			for _, c := range channels {
				cPKs = append(cPKs, c.PK)
			}
			rResV, err := db.NewRetrieve().WhereChannels(cPKs...).WhereTimeRange(cesium.TimeRangeMax).Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			segments, err := seg.StreamRetrieve{Res: rResV}.All()
			Expect(err).ToNot(HaveOccurred())
			Expect(segments).To(HaveLen(200))
		})
	})
})
