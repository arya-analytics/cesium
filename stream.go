package caesium

import "context"

type streamResponse interface {
	Error() error
}

type stream[REQ any, RES streamResponse] struct {
	req chan REQ
	res chan RES
}

func (s stream[REQ, RES]) pipe(ctx context.Context, action func(REQ) RES) {
	for {
		select {
		case <-ctx.Done():
			return
		case t, ok := <-s.req:
			if !ok {
				return
			}
			if r := action(t); r.Error() != nil {
				s.res <- r
			}
		}
	}
}

func setStream[REQ any, RES streamResponse](q query, s stream[REQ, RES]) {
	q.set(streamOptKey, s)
}

func getStream[REQ any, RES streamResponse](q query) stream[REQ, RES] {
	s, ok := getOpt[stream[REQ, RES]](q, streamOptKey)
	if !ok {
		panic("stream is not defined on query. this is a bug.")
	}
	return s
}
