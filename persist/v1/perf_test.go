package v1_test

import (
	"caesium/persist/keyfs"
	v1 "caesium/persist/v1"
	"caesium/pk"
	"caesium/telem"
	"caesium/util/testutil"
	"encoding/binary"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"math"
)

var _ = Describe("Perf", func() {
	Describe("Segment flush", func() {
		It("Should write to disk quickly", func() {
			fs := keyfs.New(keyfs.NewOSFileSource("testdata"))
			fpk := pk.New()
			cpk := pk.New()
			b := testutil.RandomFloat64Bytes(100000)
			s := v1.Segment{
				ChannelPK: cpk,
				Size:      uint64(len(b)),
				StartTS:   0,
				Data:      b,
			}
			f, err := fs.Acquire(fpk)
			Expect(err).ToNot(HaveOccurred())
			testutil.RunDurationExp("io_write", 100, func() {
				Expect(s.flush(f)).To(Succeed())
			})
			fs.Release(fpk)
			Expect(fs.Delete(fpk)).To(Succeed())
		})
	})
	Describe("Segment Read", func() {
		It("Should read the segment quickly", func() {
			fs := keyfs.New(keyfs.NewOSFileSource("testdata"))
			fpk := pk.New()
			cpk := pk.New()
			b := testutil.RandomFloat64Bytes(100000)
			s := v1.Segment{
				ChannelPK: cpk,
				Size:      uint64(len(b)),
				StartTS:   0,
				Data:      b,
			}
			f, err := fs.Acquire(fpk)
			Expect(err).ToNot(HaveOccurred())
			Expect(s.flush(f)).To(Succeed())
			testutil.RunDurationExp("io_read", 1000, func() {
				r := &v1.Retrieve{
					ChannelPK: cpk,
					TR:        telem.TimeRange{Start: telem.TimeStamp(0), End: telem.TimeStamp(0)},
				}
				Expect(r.Exec(ctx, f)).To(Succeed())
				Expect(len(r.Results)).To(Equal(1))
				Expect(math.Float64frombits(binary.LittleEndian.Uint64(r.Results[0].Data[len(r.Results[0].Data)-8:]))).To(Equal(float64(99999)))
			})
			fs.Release(fpk)
			Expect(fs.Delete(fpk)).To(Succeed())
		})
	})
})
