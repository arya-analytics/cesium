package persist_test

import (
	"caesium/persist"
	"caesium/persist/keyfs"
	v1 "caesium/persist/v1"
	"caesium/pk"
	"caesium/util/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Perf", func() {
	Describe("Writing lots of segments", func() {
		Context("Single file", func() {
			Specify("Writing segments using independent operations", func() {
				pst := persist.New(keyfs.New(keyfs.NewOSFileSource("testdata")))
				fpk := pk.New()
				cpk := pk.New()
				b := testutil.RandomFloat64Bytes(100000)
				var ops []persist.Operation
				for i := 0; i < 100; i++ {
					ops = append(ops, &v1.Create{
						FilePK: fpk,
						Segments: []v1.Segment{
							{
								ChannelPK: cpk,
								Data:      b,
								Size:      uint64(len(b)),
							},
						},
					})
				}
				var errors []error
				testutil.RunDurationExp("segment_write", 10, func() {
					errors = pst.Exec(ctx, ops...)
				})
				for _, err := range errors {
					Expect(err).To(BeNil())
				}
			})
			Specify("Writing segments using a single operation", func() {
				pst := persist.New(keyfs.New(keyfs.NewOSFileSource("testdata")))
				fpk := pk.New()
				cpk := pk.New()
				b := testutil.RandomFloat64Bytes(100000)
				op := &v1.Create{
					FilePK: fpk,
				}
				for i := 0; i < 100; i++ {
					op.Segments = append(op.Segments, v1.Segment{
						ChannelPK: cpk,
						Data:      b,
						Size:      uint64(len(b)),
					})
				}
				var errors []error
				testutil.RunDurationExp("segment_write", 10, func() {
					errors = pst.Exec(ctx, op)
				})
				for _, err := range errors {
					Expect(err).To(BeNil())
				}
			})
		})
	})
})
