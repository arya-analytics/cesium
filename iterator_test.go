package cesium_test

import (
	"github.com/arya-analytics/cesium"
	"github.com/arya-analytics/cesium/internal/testutil/seg"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/telem"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
)

var _ = Describe("Iterator", func() {
	var (
		db cesium.DB
	)
	BeforeEach(func() {
		var err error
		log := zap.NewNop()
		db, err = cesium.Open("",
			cesium.MemBacked(),
			cesium.WithLogger(log),
		)
		Expect(err).ToNot(HaveOccurred())
	})
	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
	})
	Context("Single Channel", func() {
		var (
			key     cesium.ChannelKey
			channel cesium.Channel
		)
		BeforeEach(func() {
			channel = cesium.Channel{
				DataRate: 25 * cesium.Hz,
				DataType: cesium.Float64,
			}
			var err error
			key, err = db.CreateChannel(channel)
			channel.Key = key
			Expect(err).ToNot(HaveOccurred())

			req, res, err := db.NewCreate().WhereChannels(key).Stream(ctx)
			stc := &seg.StreamCreate{
				Req: req,
				Res: res,
				SequentialFactory: seg.NewSequentialFactory(
					&seg.SequentialFloat64Factory{},
					10*cesium.Second,
					channel,
				),
			}
			Expect(err).ToNot(HaveOccurred())
			stc.CreateCRequestsOfN(100, 2)

		})
		Describe("First", func() {
			It("Should return the first segment in the iterator", func() {
				iter, err := db.NewRetrieve().
					WhereChannels(key).
					WhereTimeRange(cesium.TimeRangeMax).
					Iterate(ctx)
				Expect(err).To(BeNil())
				stream := confluence.NewStream[cesium.RetrieveResponse](1)
				iter.OutTo(stream)
				Expect(iter.First()).To(BeTrue())
				res := <-stream.Outlet()
				Expect(res.Error).ToNot(HaveOccurred())
				Expect(res.Segments).To(HaveLen(1))
				Expect(iter.View()).To(Equal(telem.TimeRange{
					Start: telem.TimeStampMin,
					End:   telem.TimeStampMin.Add(10 * cesium.Second),
				}))
				Expect(res.Segments[0].Start).To(Equal(telem.TimeStampMin))
				Expect(res.Segments[0].Sugar(channel).UnboundedRange()).To(Equal(
					telem.TimeRange{
						Start: telem.TimeStampMin,
						End:   telem.TimeStampMin.Add(10 * cesium.Second),
					}))
				Expect(iter.Close()).To(Succeed())
			})
		})
		Describe("NextSpan", func() {
			It("Should return the correct span in the iterator", func() {
				iter, err := db.NewRetrieve().
					WhereChannels(key).
					WhereTimeRange(cesium.TimeRange{
						Start: telem.TimeStamp(5 * cesium.Second),
						End:   telem.TimeStampMax,
					}).Iterate(ctx)
				Expect(err).ToNot(HaveOccurred())
				stream := confluence.NewStream[cesium.RetrieveResponse](1)
				iter.OutTo(stream)
				Expect(iter.SeekFirst()).To(BeTrue())
				Expect(iter.View()).To(Equal(telem.TimeRange{
					Start: telem.TimeStamp(5 * cesium.Second),
					End:   telem.TimeStamp(5 * cesium.Second),
				}))
				Expect(iter.NextSpan(5 * cesium.Second)).To(BeTrue())
				res := <-stream.Outlet()
				Expect(res.Error).ToNot(HaveOccurred())
				Expect(iter.View()).To(Equal(telem.TimeRange{
					Start: telem.TimeStamp(5 * cesium.Second),
					End:   telem.TimeStamp(10 * cesium.Second),
				}))
				Expect(res.Segments).To(HaveLen(1))
				Expect(res.Segments[0].Start).To(Equal(telem.TimeStamp(5 * cesium.Second)))
				Expect(res.Segments[0].Sugar(channel).UnboundedRange()).To(Equal(telem.TimeRange{
					Start: telem.TimeStamp(5 * cesium.Second),
					End:   telem.TimeStamp(10 * cesium.Second),
				}))
				Expect(iter.Close()).To(Succeed())
			})
		})
	})

})
