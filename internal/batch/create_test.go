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

type CreateOperation struct {
	channelKey int
	fileKey    int
}

func (c CreateOperation) Context() context.Context {
	return context.Background()
}

func (c CreateOperation) FileKey() int {
	return c.fileKey
}

func (c CreateOperation) Exec(f kfs.File[int]) {
	if _, err := f.Write([]byte("hello")); err != nil {
		panic(err)
	}
}

func (c CreateOperation) SendError(err error) {
	panic(err)
}

func (c CreateOperation) ChannelKey() int {
	return c.channelKey
}

var _ = Describe("Create", func() {
	var (
		req chan []batch.CreateOperation[int, int]
		res <-chan []batch.Operation[int]
		s   shut.Shutdown
	)
	BeforeEach(func() {
		req = make(chan []batch.CreateOperation[int, int])
		s = shut.New()
		res = batch.Pipe[int, batch.CreateOperation[int, int]](req, s, &batch.Create[int, int]{})
	})
	It("Should batch simple operations into one", func() {
		req <- []batch.CreateOperation[int, int]{
			CreateOperation{},
			CreateOperation{},
			CreateOperation{},
			CreateOperation{},
		}
		wg := sync.WaitGroup{}
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
		req <- []batch.CreateOperation[int, int]{
			CreateOperation{1, 1},
			CreateOperation{1, 2},
			CreateOperation{1, 1},
			CreateOperation{1, 2},
		}
		wg := sync.WaitGroup{}
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
		Expect(responses[0]).To(HaveLen(2))
		Expect(responses[1]).To(HaveLen(2))
	})
	It("Should batch operations by channel key", func() {
		req <- []batch.CreateOperation[int, int]{
			CreateOperation{1, 1},
			CreateOperation{2, 1},
			CreateOperation{1, 1},
			CreateOperation{2, 1},
		}
		wg := sync.WaitGroup{}
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
		Expect(responses[0]).To(HaveLen(2))
		Expect(responses[1]).To(HaveLen(2))
	})
})
