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

var _ = FDescribe("Iterator", func() {
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
	Describe("The Basics", func() {
		BeforeEach(func() {
			req, res, err := db.NewCreate().WhereChannels(cpk).Stream(ctx)
			stc := &seg.StreamCreate{
				Req:               req,
				Res:               res,
				SequentialFactory: seg.NewSequentialFactory(seg.RandFloat64, 10*cesium.Second, c),
			}
			Expect(err).ToNot(HaveOccurred())
			stc.CreateCRequestsOfN(10, 2)
			Expect(stc.CloseAndWait()).To(Succeed())
		})
		Describe("First", func() {
			It("Should read the first Segment", func() {
				iter, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(cesium.TimeRangeMax).NewIter()
				Expect(err).ToNot(HaveOccurred())
				stream := confluence.NewStream[cesium.RetrieveResponse](10)
				iter.OutTo(stream)
				iter.First()
				s1 := (<-stream.Outlet()).Segments[0].Sugar(c, cesium.TimeRangeMax)
				Expect(s1.Data).To(HaveLen(80))
				Expect(s1.Start()).To(Equal(telem.TimeStamp(0)))
				Expect(iter.Close()).To(Succeed())
			})
		})
		Describe("Next", func() {
			It("Should return the next Segment", func() {
				iter, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(cesium.TimeRangeMax).NewIter()
				Expect(err).ToNot(HaveOccurred())
				stream := confluence.NewStream[cesium.RetrieveResponse](10)
				iter.OutTo(stream)
				iter.First()
				s1 := (<-stream.Outlet()).Segments[0].Sugar(c, cesium.TimeRangeMax)
				Expect(s1.Data).To(HaveLen(80))
				Expect(s1.Start()).To(Equal(telem.TimeStamp(0)))
				iter.Next()
				s2 := (<-stream.Outlet()).Segments[0].Sugar(c, cesium.TimeRangeMax)
				Expect(s2.Data).To(HaveLen(80))
				Expect(s2.Start()).To(Equal(telem.TimeStamp(10 * cesium.Second)))
				iter.Next()
				s3 := (<-stream.Outlet()).Segments[0].Sugar(c, cesium.TimeRangeMax)
				Expect(s3.Data).To(HaveLen(80))
				Expect(s3.Start()).To(Equal(telem.TimeStamp(20 * cesium.Second)))
				Expect(iter.Close()).To(Succeed())
			})
		})
		Describe("NextSpan", func() {
			It("Should return the correct segments", func() {
				iter, err := db.NewRetrieve().WhereChannels(cpk).WhereTimeRange(cesium.TimeRangeMax).NewIter()
				Expect(err).ToNot(HaveOccurred())
				stream := confluence.NewStream[cesium.RetrieveResponse](10)
				iter.OutTo(stream)
				iter.First()
				s1 := (<-stream.Outlet()).Segments[0].Sugar(c, cesium.TimeRangeMax)
				Expect(s1.Data).To(HaveLen(80))
				Expect(s1.Start()).To(Equal(telem.TimeStamp(0)))
				iter.NextSpan(30 * cesium.Second)
				s2 := (<-stream.Outlet()).Segments[0].Sugar(c, cesium.TimeRangeMax)
				Expect(s2.Data).To(HaveLen(80))
				Expect(s2.Start()).To(Equal(telem.TimeStamp(10 * cesium.Second)))
				s3 := (<-stream.Outlet()).Segments[0].Sugar(c, cesium.TimeRangeMax)
				Expect(s3.Data).To(HaveLen(80))
				Expect(s3.Start()).To(Equal(telem.TimeStamp(20 * cesium.Second)))
				Expect(iter.Close()).To(Succeed())
			})
		})
	}
})
