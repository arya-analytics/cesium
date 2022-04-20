package cesium

type streamResponse interface {
	Error() error
}

type stream[REQ any, RES streamResponse] struct {
	req     chan REQ
	res     chan RES
	doneRes RES
}

func setStream[REQ any, RES streamResponse](q query, s *stream[REQ, RES]) {
	q.set(streamOptKey, s)
}

func getStream[REQ any, RES streamResponse](q query) *stream[REQ, RES] {
	s, ok := getOpt[*stream[REQ, RES]](q, streamOptKey)
	if !ok {
		panic("stream is not defined on query. this is a bug.")
	}
	return s
}
