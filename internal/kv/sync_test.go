package kv_test

import (
	"cesium/internal/binary"
	"cesium/internal/kv"
	"cesium/internal/kv/pebblekv"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"io"
)

type flushLoader struct {
	bytes []byte
}

func (f flushLoader) Flush(w io.Writer) error {
	return binary.Write(w, f.bytes)
}

func (f *flushLoader) Load(r io.Reader) error {
	return binary.Read(r, &f.bytes)
}

var _ = Describe("Sync", Ordered, func() {
	var (
		kve kv.KV
	)
	BeforeAll(func() {
		db, err := pebble.Open("", &pebble.Options{FS: vfs.NewMem()})
		Expect(err).NotTo(HaveOccurred())
		kve = pebblekv.Wrap(db)
	})
	AfterAll(func() {
		Expect(kve.Close()).To(Succeed())
	})
	Describe("Flush", func() {
		It("Should flush correctly", func() {
			Expect(kv.Flush(kve, []byte("test-key-1"), flushLoader{bytes: []byte{1, 2, 3}})).To(Succeed())
		})
	})
	Describe("Load", func() {
		It("Should load correctly", func() {
			Expect(kv.Flush(kve, []byte("test-key-1"), flushLoader{bytes: []byte{1, 2, 3}})).To(Succeed())
			resFL := &flushLoader{}
			resFL.bytes = make([]byte, 3)
			Expect(kv.Load(kve, []byte("test-key-1"), resFL)).To(Succeed())
			Expect(resFL.bytes).To(Equal([]byte{1, 2, 3}))
		})
	})
})
