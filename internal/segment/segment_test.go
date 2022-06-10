package segment_test

import (
	"encoding/binary"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/telem"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("segment", func() {
	Describe("Key", func() {
		It("Should generate the segments key correctly", func() {
			k := segment.NewKey(channel.Key(1), telem.TimeStampMax)
			Expect(k[0]).To(Equal(byte('s')))
			Expect(binary.LittleEndian.Uint64(k[1:9])).To(Equal(uint64(telem.TimeStampMax)))
			Expect(binary.LittleEndian.Uint16(k[9:13])).To(Equal(uint16(1)))
		})
	})

})
