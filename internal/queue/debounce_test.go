package queue_test

import (
	"cesium/internal/queue"
	"cesium/shut"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"time"
)

var _ = Describe("Debounce", func() {
	var (
		req chan []int
		res chan []int
		s   shut.Shutdown
		d   *queue.Debounce[int]
	)
	BeforeEach(func() {
		req = make(chan []int)
		res = make(chan []int, 100)
		s = shut.New()
		d = &queue.Debounce[int]{req, res, s, 30 * time.Millisecond, 15}
		d.Start()
	})
	It("Should flush the queue at a specified interval", func() {
		req <- []int{1, 2, 3, 4, 5}
		req <- []int{6, 7, 8, 9, 10}
		time.Sleep(50 * time.Millisecond)
		Expect(s.Shutdown()).To(Succeed())
		var responses []int
		for v := range res {
			responses = append(responses, v...)
		}
		Expect(responses).To(Equal([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}))
	})
	It("Should flush teh queue when the threshold is reached", func() {
		req <- []int{1, 2, 3, 4, 5}
		req <- []int{6, 7, 8, 9, 10}
		req <- []int{11, 12, 13, 14, 15}
		Expect(s.Shutdown()).To(Succeed())
		var responses []int
		for v := range res {
			responses = append(responses, v...)
		}
		Expect(responses).To(Equal([]int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}))
	})
})
