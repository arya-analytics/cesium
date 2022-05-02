package persist_test

import (
	"cesium/internal/operation"
	"cesium/internal/persist"
	"cesium/kfs"
	"cesium/shut"
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"go.uber.org/zap"
	"time"
)

type BasicOperation struct{}

func (b BasicOperation) Context() context.Context {
	return context.Background()
}

func (b BasicOperation) FileKey() int {
	return 1
}

func (b BasicOperation) Exec(f kfs.File[int]) {
	if _, err := f.Write([]byte("hello")); err != nil {
		panic(err)
	}
}

func (b BasicOperation) SendError(err error) {
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
		fs = kfs.New[int]("", kfs.WithFS(kfs.NewMem()))
		p = persist.New[int, operation.Operation[int]](fs, 50, sd, zap.NewNop())
	})
	Describe("Exec", func() {
		It("Should execute an operation correctly", func() {
			b := BasicOperation{}
			p.Exec([]operation.Operation[int]{b})
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
	Describe("PipeTransform", func() {
		It("Should pipe an operation correctly", func() {
			b := BasicOperation{}
			ch := make(chan []operation.Operation[int])
			p.Pipe(ch)
			ch <- []operation.Operation[int]{b, b}
			time.Sleep(1 * time.Millisecond)
			Expect(sd.Shutdown()).To(Succeed())
			f, err := fs.Acquire(1)
			Expect(err).ToNot(HaveOccurred())
			defer fs.Release(1)
			buf := make([]byte, 10)
			_, err = f.Seek(0, 0)
			Expect(err).ToNot(HaveOccurred())
			if _, err := f.Read(buf); err != nil {
				panic(err)
			}
			Expect(string(buf)).To(Equal("hellohello"))
		})
	})
})
