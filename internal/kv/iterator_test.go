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

	AfterEach(func() { Expect(kve.Close()).To(Succeed()) })

	Describe("Even Range", func() {
		var iter *kv.Iterator
		BeforeEach(func() {
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
			iter = kv.NewIterator(kve, ch.Key, telem.TimeRange{
				Start: 0,
				End:   telem.TimeStamp(200 * time.Second),
			})
		})
		AfterEach(func() { Expect(iter.Close()).To(Succeed()) })

		Describe("First", func() {
			It("Should return the correct segment", func() {
				Expect(iter.First()).To(BeTrue())
				Expect(iter.Valid()).To(BeTrue())
				Expect(iter.Error()).ToNot(HaveOccurred())
				Expect(iter.Position()).To(Equal(telem.TimeRange{
					Start: 0,
					End:   telem.TimeStamp(100 * telem.Second),
				}))
				Expect(iter.Value().Headers).To(HaveLen(1))
				Expect(iter.Value().Range()).To(Equal(iter.Position()))
				Expect(iter.Close()).To(Succeed())
			})
		})

		Describe("Last", func() {
			It("Should return the correct segment", func() {
				Expect(iter.Last()).To(BeTrue())
				Expect(iter.Valid()).To(BeTrue())
				Expect(iter.Error()).ToNot(HaveOccurred())
				Expect(iter.Position()).To(Equal(telem.TimeRange{
					Start: telem.TimeStamp(100 * telem.Second),
					End:   telem.TimeStamp(200 * time.Second),
				}))
				Expect(iter.Value().Range()).To(Equal(iter.Position()))
				Expect(iter.Close()).To(Succeed())
			})
		})

		Describe("SeekFirst", func() {
			It("Should seek the correct position", func() {
				Expect(iter.SeekFirst()).To(BeTrue())
				Expect(iter.Valid()).To(BeFalse())
				Expect(iter.Error()).ToNot(HaveOccurred())
				Expect(iter.Position()).To(Equal(telem.TimeRange{Start: 0, End: 0}))
				Expect(iter.Value().Headers).To(HaveLen(1))
				Expect(iter.Value().Range()).To(Equal(iter.Position()))
				Expect(iter.Close()).To(Succeed())
			})
		})

		Describe("SeekLast", func() {
			It("Should seek the correct position", func() {
				Expect(iter.SeekLast()).To(BeTrue())
				Expect(iter.Valid()).To(BeFalse())
				Expect(iter.Error()).ToNot(HaveOccurred())
				Expect(iter.Position()).To(Equal(telem.TimeStamp(200 * time.Second).SpanRange(0)))
			})
		})

		Describe("Seek", func() {

			Context("Within Range", func() {
				It("Should seek the correct position", func() {
					Expect(iter.Seek(telem.TimeStamp(50 * time.Second))).To(BeTrue())
					Expect(iter.Valid()).To(BeFalse())
					Expect(iter.Error()).ToNot(HaveOccurred())
				})
			})

			Context("Before Range", func() {
				It("Should return false", func() {
					Expect(iter.Seek(telem.TimeStamp(-5 * time.Second))).To(BeFalse())
					Expect(iter.Valid()).To(BeFalse())
					Expect(iter.Error()).ToNot(HaveOccurred())
				})
			})

			Context("After Range", func() {
				It("Should return true", func() {
					Expect(iter.Seek(telem.TimeStamp(500 * time.Second))).To(BeFalse())
					Expect(iter.Valid()).To(BeFalse())
					Expect(iter.Error()).ToNot(HaveOccurred())
				})
			})

			Describe("Next", func() {
				It("Should return the segment that contains the position", func() {
					Expect(iter.Seek(telem.TimeStamp(50 * time.Second))).To(BeTrue())
					Expect(iter.Next()).To(BeTrue())
					Expect(iter.Value().Headers[0].Start).To(Equal(telem.TimeStamp(0 * time.Second)))
				})
			})

		})

		Describe("Next", func() {

			Context("At First", func() {
				It("Should return the correct segment", func() {
					Expect(iter.SeekFirst()).To(BeTrue())
					Expect(iter.Next()).To(BeTrue())
					Expect(iter.Error()).ToNot(HaveOccurred())
					Expect(iter.Position()).To(Equal(telem.TimeRange{
						Start: 0,
						End:   telem.TimeStamp(100 * time.Second),
					}))

				})
			})

			Context("At Last", func() {
				It("Should return false", func() {
					Expect(iter.SeekLast()).To(BeTrue())
					Expect(iter.Next()).To(BeFalse())
				})
			})

			Context("Approaching Last", func() {
				It("Should return false when it reaches last", func() {
					Expect(iter.First()).To(BeTrue())
					Expect(iter.Next()).To(BeTrue())
					Expect(iter.Next()).To(BeFalse())
				})
			})

		})

		Describe("Prev", func() {

			Context("At Last", func() {
				It("Should return the correct segment", func() {
					Expect(iter.SeekLast()).To(BeTrue())
					Expect(iter.Prev()).To(BeTrue())
					Expect(iter.Error()).ToNot(HaveOccurred())
					Expect(iter.Position()).To(Equal(telem.TimeRange{
						Start: telem.TimeStamp(100 * time.Second),
						End:   telem.TimeStamp(200 * time.Second),
					}))
				})
			})

			Context("At first", func() {
				It("Should return false", func() {
					Expect(iter.SeekFirst()).To(BeTrue())
					Expect(iter.Prev()).To(BeFalse())
				})
			})

			Context("Approaching first", func() {
				It("Should return false when it reaches first", func() {
					Expect(iter.Last()).To(BeTrue())
					Expect(iter.Prev()).To(BeTrue())
					Expect(iter.Prev()).To(BeFalse())
				})
			})

		})

	})

	Context("Uneven Range", func() {
		Context("First Range Starts After, Last Range Ends After", func() {

			var iter *kv.Iterator
			BeforeEach(func() {
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
				iter = kv.NewIterator(kve, ch.Key, telem.TimeRange{
					Start: telem.TimeStamp(5 * time.Second),
					End:   telem.TimeStamp(200 * time.Second),
				})
			})
			AfterEach(func() { Expect(iter.Close()).To(Succeed()) })

			Describe("First", func() {
				It("Should return the correct segment", func() {
					Expect(iter.First()).To(BeTrue())
					Expect(iter.Valid()).To(BeTrue())
					Expect(iter.Error()).ToNot(HaveOccurred())
					Expect(iter.Position()).To(Equal(telem.TimeRange{
						Start: telem.TimeStamp(10 * time.Second),
						End:   telem.TimeStamp(110 * time.Second),
					}))
					Expect(iter.Value().Headers).To(HaveLen(1))
					Expect(iter.Value().Range()).To(Equal(iter.Position()))
					Expect(iter.Close()).To(Succeed())
				})
			})

			Describe("Last", func() {
				It("Should return the correct segment", func() {
					Expect(iter.Last()).To(BeTrue())
					Expect(iter.Valid()).To(BeTrue())
					Expect(iter.Error()).ToNot(HaveOccurred())
					Expect(iter.Position()).To(Equal(telem.TimeRange{
						Start: telem.TimeStamp(110 * time.Second),
						End:   telem.TimeStamp(200 * time.Second),
					}))
					Expect(iter.Value().Range()).To(Equal(iter.Position()))
				})
			})

			Describe("SeekTo", func() {
				It("Should seek to the correct position", func() {
					Expect(iter.SeekFirst()).To(BeTrue())
					Expect(iter.Valid()).To(BeFalse())
					Expect(iter.Error()).ToNot(HaveOccurred())
					Expect(iter.Position()).To(Equal(telem.TimeStamp(10 * time.Second).SpanRange(0)))
				})

				It("Should return the correct next segment", func() {
					Expect(iter.SeekFirst()).To(BeTrue())
					Expect(iter.Next()).To(BeTrue())
					Expect(iter.Value().Headers).To(HaveLen(1))
					Expect(iter.Position()).To(Equal(telem.TimeRange{
						Start: telem.TimeStamp(10 * time.Second),
						End:   telem.TimeStamp(110 * time.Second),
					}))
					Expect(iter.Value().Range()).To(Equal(iter.Position()))
				})
			})

		})
	})

	Context("Invalid Range", func() {
		var iter *kv.Iterator
		BeforeEach(func() {
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
			iter = kv.NewIterator(kve, ch.Key, telem.TimeRange{
				Start: telem.TimeStamp(5 * time.Second),
				End:   telem.TimeStamp(210 * time.Second),
			})
		})
		AfterEach(func() { Expect(iter.Close()).To(Succeed()) })

		Describe("First", func() {
			It("Should return false", func() {
				Expect(iter.Error()).To(HaveOccurred())
				Expect(iter.Error()).To(MatchError("[cesium.kv] - range has no data"))
				Expect(iter.First()).To(BeFalse())
				Expect(iter.Valid()).To(BeFalse())
				Expect(iter.Value().Headers).To(HaveLen(0))
				Expect(iter.Value().Range()).To(Equal(telem.TimeRangeZero))
				Expect(iter.Close()).To(Succeed())
			})
		})

		Describe("SeekFirst", func() {
			It("Should return false", func() {
				Expect(iter.SeekFirst()).To(BeFalse())
				Expect(iter.Close()).To(Succeed())
			})
		})

	})

	Describe("NextSpan", func() {
		Context("Starting at the beginning of a segment", func() {
			Context("Reading an entire segment", func() {
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
					Expect(iter.SeekFirst()).To(BeTrue())
					Expect(iter.NextSpan(100 * telem.Second)).To(BeTrue())
					Expect(iter.Valid()).To(BeTrue())
					Expect(iter.Error()).ToNot(HaveOccurred())
					Expect(iter.Position()).To(Equal(telem.TimeRange{
						Start: 0,
						End:   telem.TimeStamp(100 * telem.Second),
					}))
					Expect(iter.Value().Range()).To(Equal(iter.Position()))
					Expect(iter.Value().Headers).To(HaveLen(1))
					Expect(iter.Close()).To(Succeed())
				})
			})
			Context("Reading a partial segment", func() {

				It("Should return the partial segment correctly", func() {
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
					Expect(iter.SeekFirst()).To(BeTrue())
					Expect(iter.NextSpan(50 * telem.Second)).To(BeTrue())
					Expect(iter.Valid()).To(BeTrue())
					Expect(iter.Error()).ToNot(HaveOccurred())
					Expect(iter.Position()).To(Equal(telem.TimeRange{
						Start: 0,
						End:   telem.TimeStamp(50 * telem.Second),
					}))
					Expect(iter.Value().Range()).To(Equal(iter.Position()))
					Expect(iter.Value().Headers).To(HaveLen(1))
					Expect(iter.Value().Headers[0].Start).To(Equal(telem.TimeStamp(0)))
					Expect(iter.Prev()).To(BeFalse())
					Expect(iter.Close()).To(Succeed())
				})
			})
		})

		Context("Reading multiple segments", func() {
			It("Should return the segments correctly", func() {
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
					{
						ChannelKey: ch.Key,
						Start:      telem.TimeStamp(200 * time.Second),
						Size:       100,
					},
				})).To(Succeed())
				iter := kv.NewIterator(kve, ch.Key, telem.TimeRange{
					Start: 0,
					End:   telem.TimeStamp(300 * time.Second),
				})
				Expect(iter.SeekFirst()).To(BeTrue())
				Expect(iter.NextSpan(200 * telem.Second)).To(BeTrue())
				Expect(iter.Valid()).To(BeTrue())
				Expect(iter.Position()).To(Equal(telem.TimeRange{
					Start: 0,
					End:   telem.TimeStamp(200 * time.Second),
				}))
				Expect(iter.Value().Range()).To(Equal(iter.Position()))
				Expect(iter.Value().Headers).To(HaveLen(2))
				Expect(iter.Close()).To(Succeed())
			})
		})
	})
})
