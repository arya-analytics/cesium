package wg

type Slice[T any] struct {
	Items []T
	Done  chan struct{}
}

func (s Slice[T]) Wait() {
	c := 0
	for range s.Done {
		c++
		if c >= len(s.Items) {
			break
		}
	}
}
