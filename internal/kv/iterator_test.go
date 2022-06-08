package kv_test

import (
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/segment"
	kvx "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/memkv"
	"github.com/arya-analytics/x/telem"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Iterator", func() {
	var (
		kve      kvx.KV
		ch       channel.Channel
		headerKV *kv.Header
	)
	BeforeEach(func() {
		ch = channel.Channel{Key: 1, DataRate: 1, DataType: 1}
		kve = memkv.Open()
		headerKV = kv.NewHeader(kve)
		chKV := kv.NewChannel(kve)
		Expect(chKV.Set(ch)).To(Succeed())
	})
	Describe("First", func() {
		Context("Even Range", func() {
			It("Should return the correct segment", func() {
				Expect(headerKV.SetMultiple([]segment.Header{
					{
						ChannelKey: ch.Key,
						Start:      0,
						Size:       100,
					},
					{
						ChannelKey: ch.Key,
						Start:      telem.TimeStamp(100 * telem.Second),
						Size:       100,
					},
				})).To(Succeed())

				iter := kv.NewIterator(kve, ch.Key, telem.TimeRange{
					Start: 0,
					End:   telem.TimeStamp(200 * time.Second),
				})

				Expect(iter.First()).To(BeTrue())
				Expect(iter.Position()).To(Equal(telem.TimeRange{
					Start: 0,
					End:   telem.TimeStamp(100 * telem.Second),
				}))
				Expect(iter.Value().Headers).To(HaveLen(1))
				Expect(iter.Value().Range()).To(Equal(iter.Position()))
			})
		})

		Context("Uneven Range", func() {
			It("Should return the correct segment", func() {
				Expect(headerKV.SetMultiple([]segment.Header{
					{
						ChannelKey: ch.Key,
						Start:      telem.TimeStamp(10 * time.Second),
						Size:       100,
					},
					{
						ChannelKey: ch.Key,
						Start:      telem.TimeStamp(110 * time.Second),
						Size:       100,
					},
				})).To(Succeed())

				iter := kv.NewIterator(kve, ch.Key, telem.TimeRange{
					Start: telem.TimeStamp(5 * time.Second),
					End:   telem.TimeStamp(210 * time.Second),
				})

				Expect(iter.First()).To(BeTrue())
				Expect(iter.Position()).To(Equal(telem.TimeRange{
					Start: telem.TimeStamp(10 * time.Second),
					End:   telem.TimeStamp(110 * time.Second),
				}))
				Expect(iter.Value().Headers).To(HaveLen(1))
				Expect(iter.Value().Range()).To(Equal(iter.Position()))
			})
		})

		Context("Invalid Range", func() {
			It("Should return false", func() {
				Expect(headerKV.SetMultiple([]segment.Header{
					{
						ChannelKey: ch.Key,
						Start:      telem.TimeStamp(220 * time.Second),
						Size:       100,
					},
					{
						ChannelKey: ch.Key,
						Start:      telem.TimeStamp(320 * time.Second),
						Size:       100,
					},
				})).To(Succeed())

				iter := kv.NewIterator(kve, ch.Key, telem.TimeRange{
					Start: telem.TimeStamp(5 * time.Second),
					End:   telem.TimeStamp(210 * time.Second),
				})

				Expect(iter.First()).To(BeFalse())
				Expect(iter.Valid()).To(BeFalse())
				Expect(iter.Value().Headers).To(HaveLen(0))
				Expect(iter.Value().Range()).To(Equal(telem.TimeRangeZero))
				Expect(iter.Error()).To(HaveOccurred())
				Expect(iter.Error()).To(MatchError("[cesium.kv] - range has no data"))
			})
		})
	})

})
