package cesium_test

import (
	"github.com/arya-analytics/cesium"
	"github.com/arya-analytics/cesium/internal/testutil/seg"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"sync"
)

var _ = Describe("Retrieve", func() {
	Context("Single channel", func() {
		var (
			db  cesium.DB
			cpk int16
			c   cesium.Channel
			log *zap.Logger
		)
		BeforeEach(func() {
			var err error
			log = zap.NewNop()
			db, err = cesium.Open("testdata", cesium.MemBacked(), cesium.WithLogger(log))
			Expect(err).ToNot(HaveOccurred())
			c, err = db.NewCreateChannel().WithRate(cesium.Hz).WithType(cesium.Float64).Exec(ctx)
			Expect(err).ToNot(HaveOccurred())
			cpk = c.Key
		})
		AfterEach(func() {
			Expect(db.Close()).To(Succeed())
		})
		It("Should read the segmentKV correctly", func() {
			req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
			stc := &seg.StreamCreate{
				Req:               req,
				Res:               res,
				SequentialFactory: seg.NewSequentialFactory(seg.RandFloat64, 10*cesium.Second, c),
			}
			Expect(err).ToNot(HaveOccurred())
			stc.CreateCRequestsOfN(10, 2)
			Expect(stc.CloseAndWait()).To(Succeed())
			logrus.Info("Made it here")
			resV, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(cesium.TimeRangeMax).Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			segments, err := seg.StreamRetrieve{Res: resV}.All()
			Expect(err).ToNot(HaveOccurred())
			Expect(segments).To(HaveLen(20))
		})
		It("It should support multiple concurrent read requests", func() {
			req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			stc := &seg.StreamCreate{
				Req:               req,
				Res:               res,
				SequentialFactory: seg.NewSequentialFactory(seg.RandFloat64, 10*cesium.Second, c),
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
	Context("Multi channel", func() {
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
				req, res, err := db.NewCreate().WhereChannels(channels[i].Key).Stream(ctx)
				Expect(err).ToNot(HaveOccurred())
				stc := &seg.StreamCreate{
					Req:               req,
					Res:               res,
					SequentialFactory: seg.NewSequentialFactory(seg.RandFloat64, 10*cesium.Second, channels[i]),
				}
				stc.CreateCRequestsOfN(10, 2)
				Expect(stc.CloseAndWait()).To(Succeed())
			}
			var cPKs []int16
			for _, c := range channels {
				cPKs = append(cPKs, c.Key)
			}
			rResV, err := db.NewRetrieve().WhereChannels(cPKs...).WhereTimeRange(cesium.TimeRangeMax).Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			segments, err := seg.StreamRetrieve{Res: rResV}.All()
			Expect(err).ToNot(HaveOccurred())
			Expect(segments).To(HaveLen(200))
		})
	})
})
