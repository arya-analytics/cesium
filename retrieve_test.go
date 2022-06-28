package cesium_test

import (
	"github.com/arya-analytics/cesium"
	"github.com/arya-analytics/cesium/testutil/seg"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"sync"
)

var _ = Describe("Retrieve", func() {
	Context("Single channel", func() {
		var (
			db      cesium.DB
			key     cesium.ChannelKey
			channel cesium.Channel
			log     *zap.Logger
		)
		BeforeEach(func() {
			var err error
			log = zap.NewNop()
			db, err = cesium.Open("testdata", cesium.MemBacked(),
				cesium.WithLogger(log))
			Expect(err).ToNot(HaveOccurred())
			channel = cesium.Channel{
				DataRate: 1 * cesium.Hz,
				DataType: cesium.Float64,
			}
			key, err = db.CreateChannel(channel)
			channel.Key = key
			Expect(err).ToNot(HaveOccurred())
		})
		AfterEach(func() {
			Expect(db.Close()).To(Succeed())
		})
		It("Should read the segments correctly", func() {
			req, res, err := db.NewCreate().WhereChannels(key).Stream(ctx)
			stc := &seg.StreamCreate{
				Req: req,
				Res: res,
				SequentialFactory: seg.NewSequentialFactory(
					&seg.RandomFloat64Factory{},
					10*cesium.Second,
					channel,
				),
			}
			Expect(err).ToNot(HaveOccurred())
			stc.CreateCRequestsOfN(10, 2)
			Expect(stc.CloseAndWait()).To(Succeed())
			resV, err := db.NewRetrieve().WhereChannels(key).WhereTimeRange(cesium.TimeRangeMax).Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			segments, err := seg.StreamRetrieve{Res: resV}.All()
			Expect(err).ToNot(HaveOccurred())
			Expect(segments).To(HaveLen(20))
		})
		It("It should support multiple concurrent read requests", func() {
			req, res, err := db.NewCreate().WhereChannels(key).Stream(ctx)
			Expect(err).ToNot(HaveOccurred())
			stc := &seg.StreamCreate{
				Req: req,
				Res: res,
				SequentialFactory: seg.NewSequentialFactory(&seg.RandomFloat64Factory{}, 10*cesium.Second,
					channel),
			}
			stc.CreateCRequestsOfN(10, 2)
			Expect(stc.CloseAndWait()).To(Succeed())
			const nRequests = 10
			wg := sync.WaitGroup{}
			wg.Add(nRequests)
			for i := 0; i < nRequests; i++ {
				go func() {
					defer GinkgoRecover()
					defer wg.Done()
					rResV, err := db.NewRetrieve().
						WhereChannels(key).
						WhereTimeRange(cesium.TimeRangeMax).
						Stream(ctx)
					Expect(err).ToNot(HaveOccurred())
					if err != nil {
						return

					}
					segments, err := seg.StreamRetrieve{Res: rResV}.All()
					Expect(err).ToNot(HaveOccurred())
					Expect(segments).To(HaveLen(20))
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
				c := cesium.Channel{
					DataRate: 1 * cesium.Hz,
					DataType: cesium.Float64,
				}
				k, err := db.CreateChannel(c)
				c.Key = k
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
					Req: req,
					Res: res,
					SequentialFactory: seg.NewSequentialFactory(&seg.RandomFloat64Factory{},
						10*cesium.Second, channels[i]),
				}
				stc.CreateCRequestsOfN(10, 2)
				Expect(stc.CloseAndWait()).To(Succeed())
			}
			var cPKs []cesium.ChannelKey
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
