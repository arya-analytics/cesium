package cesium_test

//var _ = Describe("Sync", func() {
//	var (
//		db cesium.DB
//	)
//	BeforeEach(func() {
//		var err error
//		log := zap.NewNop()
//		db, err = cesium.Open("", cesium.MemBacked(), cesium.WithLogger(log))
//		Expect(err).ToNot(HaveOccurred())
//	})
//	Context("Create", func() {
//		It("Should write the segments to disk", func() {
//				WithRate(1 * cesium.Hz).
//				WithType(cesium.Float64).
//				Exec(ctx)
//			Expect(err).ToNot(HaveOccurred())
//
//			segments := []cesium.Segment{
//				{
//					ChannelKey: ch.Key,
//					Start:      cesium.Now(),
//					Data:       cesium.MarshalFloat64([]float64{1, 2, 3}),
//				},
//			}
//
//			Expect(db.Sync(
//				ctx,
//				db.NewCreate().WhereChannels(ch.Key),
//				&segments,
//			)).To(Succeed())
//
//			var resSegments []cesium.Segment
//			Expect(db.Sync(
//				ctx,
//				db.NewRetrieve().WhereChannels(ch.Key),
//				&resSegments,
//			)).To(Succeed())
//
//			Expect(resSegments).To(HaveLen(1))
//		})
//	})
//	Context("Retrieve", func() {
//		It("Should retrieve segments from disk", func() {
//			ch, err := db.CreateChannel().
//				WithRate(1 * cesium.Hz).
//				WithType(cesium.Float64).
//				Exec(ctx)
//			Expect(err).ToNot(HaveOccurred())
//
//			segments := []cesium.Segment{
//				{
//					ChannelKey: ch.Key,
//					Start:      cesium.Now(),
//					Data:       cesium.MarshalFloat64([]float64{1, 2, 3}),
//				},
//				{
//					ChannelKey: ch.Key,
//					Start:      cesium.Now().Add(3 * cesium.Second),
//					Data:       cesium.MarshalFloat64([]float64{4, 5, 6}),
//				},
//			}
//
//			Expect(db.Sync(
//				ctx,
//				db.NewCreate().WhereChannels(ch.Key),
//				&segments,
//			)).To(Succeed())
//
//			var resSegments []cesium.Segment
//			Expect(db.Sync(
//				ctx,
//				db.NewRetrieve().WhereChannels(ch.Key).WhereTimeRange(cesium.TimeRange{
//					Start: cesium.Now().Add(-10 * cesium.Second),
//					End:   cesium.Now().Add(10 * cesium.Second),
//				}),
//				&resSegments,
//			)).To(Succeed())
//
//			Expect(resSegments).To(HaveLen(2))
//			cesium.Sort(resSegments)
//			Expect(resSegments[0].Start).To(Equal(segments[0].Start))
//		})
//	})
//})
