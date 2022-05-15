package persist_test

import (
	"context"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/internal/persist"
	"github.com/arya-analytics/cesium/kfs"
	"github.com/arya-analytics/cesium/shut"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

type BasicOperation struct {
	executed bool
}

func (b *BasicOperation) Context() context.Context {
	return context.Background()
}

func (b *BasicOperation) FileKey() int {
	return 1
}

func (b *BasicOperation) Exec(f kfs.File[int]) {
	b.executed = true
	if _, err := f.Write([]byte("hello")); err != nil {
		panic(err)
	}
}

func (b *BasicOperation) SendError(err error) {
	panic(err)
}

var _ = Describe("Persist", func() {
	var (
		p  *persist.Persist[int, operation.Operation[int]]
		sd shut.Shutdown
		fs kfs.FS[int]
	)
	BeforeEach(func() {
		sd = shut.New()
		var err error
		fs, err = kfs.New[int]("testdata", kfs.WithFS(kfs.NewMem()))
		Expect(err).ToNot(HaveOccurred())
		p = persist.New[int, operation.Operation[int]](fs, persist.Config{
			NumWorkers: 50,
			Shutdown:   sd,
		})
	})
	Describe("QExec", func() {
		It("Should execute an operation correctly", func() {
			b := &BasicOperation{}
			ops := make(chan []operation.Operation[int])
			p.Pipe(ops)
			ops <- []operation.Operation[int]{b}
			close(ops)
			// Read the file.
			Expect(sd.Shutdown()).To(Succeed())
			f, err := fs.Acquire(1)
			Expect(err).ToNot(HaveOccurred())
			fs.Release(1)
			buf := make([]byte, 5)
			_, err = f.Seek(0, 0)
			Expect(err).ToNot(HaveOccurred())
			if _, err := f.Read(buf); err != nil {
				panic(err)
			}
			Expect(string(buf)).To(Equal("hello"))
		})
	})
	Describe("Shutdown", func() {
		It("Should execute all operations before shutting down", func() {
			b := &BasicOperation{}
			ops := make(chan []operation.Operation[int])
			p.Pipe(ops)
			ops <- []operation.Operation[int]{b}
			close(ops)
			Expect(sd.Shutdown()).To(Succeed())
			Expect(b.executed).To(BeTrue())
		})
	})
})
