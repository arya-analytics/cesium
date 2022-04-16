package v1_test

import (
	"caesium/persist"
	"caesium/persist/keyfs"
	v1 "caesium/persist/v1"
	"caesium/pk"
	"caesium/telem"
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var ctx = context.Background()

var _ = Describe("V1", func() {
	Describe("Segments", func() {
		Describe("Creating Segments", func() {
			Context("Data", func() {
				It("Should write the correct segment data", func() {
					fs := keyfs.New(keyfs.NewOSFileSource("testdata"))
					pst := persist.New(fs)
					fpk := pk.New()
					cpk := pk.New()
					d := []byte("hello world")
					errors := pst.Exec(ctx, &v1.Create{
						FilePK: fpk,
						Segments: []v1.Segment{
							{
								ChannelPK: cpk,
								Size:      uint64(len(d)),
								Data:      d,
							},
						},
					})
					Expect(errors[0]).To(BeNil())
					r := &v1.Retrieve{FilePK: fpk, ChannelPK: cpk}
					errors = pst.Exec(ctx, r)
					Expect(errors[0]).To(BeNil())
					Expect(r.Results[0].Data).To(Equal(d))
					errors = pst.Exec(ctx, &v1.Create{
						FilePK: fpk,
						Segments: []v1.Segment{
							{
								ChannelPK: cpk,
								Size:      uint64(len(d)),
								Data:      d,
							},
						},
					})
					Expect(errors[0]).To(BeNil())
					r = &v1.Retrieve{FilePK: fpk, ChannelPK: cpk}
					errors = pst.Exec(ctx, r)
					Expect(errors[0]).To(BeNil())
					Expect(r.Results[0].Data).To(Equal(d))
					Expect(r.Results[1].Data).To(Equal(d))
					Expect(fs.Delete(fpk)).To(Succeed())
				})
			})
			Describe("Meta Data", func() {
				var (
					fs  keyfs.FS
					s   v1.Segment
					fpk pk.PK
					cpk pk.PK
				)
				BeforeEach(func() {
					fs = keyfs.New(keyfs.NewOSFileSource("testdata"))
					pst := persist.New(fs)
					fpk = pk.New()
					cpk = pk.New()
					d := []byte("hello world")
					errors := pst.Exec(ctx, &v1.Create{
						FilePK: fpk,
						Segments: []v1.Segment{
							{
								ChannelPK: cpk,
								Size:      uint64(len(d)),
								Data:      d,
								StartTS:   telem.TimeStamp(100),
							},
						},
					})
					Expect(errors[0]).To(BeNil())
					r := &v1.Retrieve{
						FilePK:    fpk,
						ChannelPK: cpk,
						TR:        telem.TimeRange{Start: telem.TimeStamp(100), End: telem.TimeStamp(200)},
					}

					errors = pst.Exec(ctx, r)
					Expect(errors[0]).To(BeNil())
					s = r.Results[0]
				})
				AfterEach(func() {
					Expect(fs.Delete(fpk)).To(Succeed())
				})
				It("Should write the correct segment channel PK", func() {
					Expect(s.ChannelPK).To(Equal(cpk))
				})
				It("Should write the correct segment size", func() {
					Expect(s.Size).To(Equal(uint64(len("hello world"))))
				})
				It("Should write teh correct start timestamp", func() {
					Expect(s.StartTS).To(Equal(telem.TimeStamp(100)))
				})
			})
		})
	})
})
