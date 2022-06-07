package iterator_test

import (
	"github.com/arya-analytics/cesium"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/iterator"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/binary"
	kvc "github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/kv/memkv"
	"github.com/arya-analytics/x/telem"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Header", func() {
	var (
		kv     kvc.KV
		writer *segment.HeaderWriter
		ch     channel.Channel
		iter   *iterator.Header
		ed     binary.EncoderDecoder
	)
	BeforeEach(func() {
		ch = channel.Channel{
			Key:      1,
			DataRate: 1,
			DataType: 1,
		}
		kv = memkv.Open()
		ed = &binary.GobEncoderDecoder{}
		writer = &segment.HeaderWriter{KV: kv, Encoder: ed}
		iter = iterator.NewHeader(kv, ch, telem.TimeRangeMax, ed)
	})
	Describe("First", func() {
		It("Should move the iterator to the first value", func() {
			Expect(writer.Write(segment.Header{
				ChannelKey: 1,
				Start:      0,
				Size:       100,
			})).To(Succeed())
			Expect(writer.Write(segment.Header{
				ChannelKey: 1,
				Start:      telem.TimeStamp(100 * telem.Second),
				Size:       100,
			})).To(Succeed())
			Expect(iter.First()).To(BeTrue())
			Expect(iter.Position()).To(Equal(telem.TimeRange{
				Start: 0,
				End:   telem.TimeStamp(100 * cesium.Second),
			}))
		})
	})
})
