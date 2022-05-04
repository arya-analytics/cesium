package cesium_test

import (
	"github.com/arya-analytics/cesium"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = FDescribe("Create", func() {
	var (
		db cesium.DB
	)
	BeforeEach(func() {
		var err error
		//log, err := zap.NewDevelopment()
		db, err = cesium.Open("", cesium.MemBacked())
		Expect(err).ToNot(HaveOccurred())
	})

	AfterEach(func() {
		Expect(db.Close()).To(Succeed())
	})

	Describe("Basic Functionality", func() {

		Context("Single Channel", func() {

			Context("Single Segment", func() {

				It("Should write the segment correctly", func() {

					By("Creating a new channel")
					ch, err := db.NewCreateChannel().WithRate(1 * cesium.Hz).WithType(cesium.Float64).Exec(ctx)
					Expect(err).ToNot(HaveOccurred())

					By("Initializing a request")
					cReq := cesium.CreateRequest{
						Segments: []cesium.Segment{
							{
								ChannelKey: ch.Key,
								Start:      cesium.Now(),
								Data:       cesium.MarshalFloat64([]float64{1}),
							},
						},
					}

					By("Opening the create query")
					req, res, err := db.NewCreate().WhereChannels(ch.Key).Stream(ctx)
					Expect(err).ToNot(HaveOccurred())

					By("Writing the segment")
					req <- cReq

					By("Closing the request pipe")
					close(req)

					By("Not returning any errors")
					for resV := range res {
						Expect(resV.Err).ToNot(HaveOccurred())
					}

					By("Retrieving the segment afterwards")
					var resSeg []cesium.Segment
					err = db.Sync(ctx, db.NewRetrieve().WhereChannels(ch.Key).WhereTimeRange(cesium.TimeRangeMax), &resSeg)
					Expect(err).ToNot(HaveOccurred())
					Expect(resSeg).To(HaveLen(1))
					Expect(resSeg[0].Start).To(Equal(cReq.Segments[0].Start))

				})

			})

			Context("Multi Segment", func() {

				It("Should write the segments correctly", func() {
					By("Creating a new channel")
					ch, err := db.NewCreateChannel().WithRate(1 * cesium.Hz).WithType(cesium.Float64).Exec(ctx)
					Expect(err).ToNot(HaveOccurred())

					By("Initializing a request")
					cReq := cesium.CreateRequest{
						Segments: []cesium.Segment{
							{
								ChannelKey: ch.Key,
								Start:      cesium.Now(),
								Data:       cesium.MarshalFloat64([]float64{1}),
							},
							{
								ChannelKey: ch.Key,
								Start:      cesium.Now().Add(1 * cesium.Second),
								Data:       cesium.MarshalFloat64([]float64{2}),
							},
						},
					}

					By("Opening the create query")
					req, res, err := db.NewCreate().WhereChannels(ch.Key).Stream(ctx)
					Expect(err).ToNot(HaveOccurred())

					By("Writing the segments")
					req <- cReq

					By("Closing the request pipe")
					close(req)

					By("Not returning any errors")
					for resV := range res {
						Expect(resV.Err).ToNot(HaveOccurred())
					}

					By("Retrieving the segments afterwards")
					var resSeg []cesium.Segment
					err = db.Sync(ctx, db.NewRetrieve().WhereChannels(ch.Key).WhereTimeRange(cesium.TimeRangeMax), &resSeg)
					Expect(err).ToNot(HaveOccurred())
					Expect(resSeg).To(HaveLen(2))
					Expect(resSeg[0].Start).To(Equal(cReq.Segments[0].Start))
					Expect(resSeg[1].Start).To(Equal(cReq.Segments[1].Start))
				})
			})

			Context("Multi Request", func() {

				It("Should write the segments correctly", func() {
					By("Creating a new channel")
					ch, err := db.NewCreateChannel().WithRate(1 * cesium.Hz).WithType(cesium.Float64).Exec(ctx)
					Expect(err).ToNot(HaveOccurred())

					By("Initializing a request")
					cReqOne := cesium.CreateRequest{
						Segments: []cesium.Segment{
							{
								ChannelKey: ch.Key,
								Start:      cesium.Now(),
								Data:       cesium.MarshalFloat64([]float64{1}),
							},
						},
					}
					cReqTwo := cesium.CreateRequest{
						Segments: []cesium.Segment{
							{
								ChannelKey: ch.Key,
								Start:      cesium.Now().Add(1 * cesium.Second),
								Data:       cesium.MarshalFloat64([]float64{2}),
							},
						},
					}

					By("Opening the create query")
					req, res, err := db.NewCreate().WhereChannels(ch.Key).Stream(ctx)
					Expect(err).ToNot(HaveOccurred())

					By("Writing the segments")
					req <- cReqOne
					req <- cReqTwo

					By("Closing the request pipe")
					close(req)

					By("Not returning any errors")
					for resV := range res {
						Expect(resV.Err).ToNot(HaveOccurred())
					}

					By("Retrieving the segments afterwards")
					var resSeg []cesium.Segment
					err = db.Sync(ctx, db.NewRetrieve().WhereChannels(ch.Key).WhereTimeRange(cesium.TimeRangeMax), &resSeg)
					Expect(err).ToNot(HaveOccurred())
					Expect(resSeg).To(HaveLen(2))
					ts := []cesium.TimeStamp{
						cReqOne.Segments[0].Start,
						cReqTwo.Segments[0].Start}
					Expect(resSeg[0].Start).To(BeElementOf(ts))
					Expect(resSeg[1].Start).To(BeElementOf(ts))
				})

			})

		})

	})

	Describe("Channel Validation", func() {
		
	})

	Describe("Segment Validation", func() {

	})

})
