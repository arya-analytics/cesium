package batch_test

import (
	"cesium/internal/batch"
	"cesium/kfs"
	"cesium/shut"
	"context"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"sync"
)

type RetrieveOperation struct {
	fileKey int
	offset  int64
}

func (r RetrieveOperation) Context() context.Context {
	return context.Background()
}

func (r RetrieveOperation) FileKey() int {
	return r.fileKey
}

func (r RetrieveOperation) Offset() int64 {
	return r.offset
}

func (r RetrieveOperation) SendError(err error) {
	panic(err)
}

func (r RetrieveOperation) Exec(f kfs.File[int]) {}

var _ = Describe("Retrieve", func() {
	var (
		req chan []batch.RetrieveOperation[int]
		res <-chan []batch.Operation[int]
		s   shut.Shutdown
	)
	BeforeEach(func() {
		req = make(chan []batch.RetrieveOperation[int])
		s = shut.New()
		res = batch.Pipe[int, batch.RetrieveOperation[int]](req, s, &batch.Retrieve[int]{})
	})
	It("Should batch simple operations into one", func() {
		req <- []batch.RetrieveOperation[int]{
			RetrieveOperation{1, 2},
			RetrieveOperation{1, 3},
			RetrieveOperation{1, 4},
			RetrieveOperation{1, 5},
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		c := 0
		go func() {
			for range res {
				c++
			}
			wg.Done()
		}()
		Expect(s.Shutdown()).To(Succeed())
		wg.Wait()
		Expect(c).To(Equal(1))
	})
	It("Should batch operations by file key", func() {
		req <- []batch.RetrieveOperation[int]{
			RetrieveOperation{1, 2},
			RetrieveOperation{1, 3},
			RetrieveOperation{2, 4},
			RetrieveOperation{2, 5},
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		var responses []batch.Operation[int]
		go func() {
			for resp := range res {
				responses = append(responses, resp...)
			}
			wg.Done()
		}()
		Expect(s.Shutdown()).To(Succeed())
		wg.Wait()
		Expect(len(responses)).To(Equal(2))
	})
	It("Should order operations by offset", func() {
		req <- []batch.RetrieveOperation[int]{
			RetrieveOperation{1, 2},
			RetrieveOperation{1, 3},
			RetrieveOperation{1, 22},
			RetrieveOperation{1, 5},
		}
		wg := &sync.WaitGroup{}
		wg.Add(1)
		var responses []batch.Operation[int]
		go func() {
			for resp := range res {
				responses = append(responses, resp...)
			}
			wg.Done()
		}()
		Expect(s.Shutdown()).To(Succeed())
		wg.Wait()
		Expect(len(responses)).To(Equal(1))
		fr := responses[0].(batch.OperationSet[int, batch.RetrieveOperation[int]])
		Expect(fr).To(HaveLen(4))
		Expect(fr[0].(RetrieveOperation).Offset()).To(Equal(int64(2)))
		Expect(fr[1].(RetrieveOperation).Offset()).To(Equal(int64(3)))
		Expect(fr[2].(RetrieveOperation).Offset()).To(Equal(int64(5)))
		Expect(fr[3].(RetrieveOperation).Offset()).To(Equal(int64(22)))
	})
})
