package cesium

type StreamResponse interface {
	Error() error
}

type stream[REQ any, RES StreamResponse] struct {
	req     chan REQ
	res     chan RES
	doneRes RES
}

func setStream[REQ any, RES StreamResponse](q query, s *stream[REQ, RES]) {
	q.set(streamOptKey, s)
}

func getStream[REQ any, RES StreamResponse](q query) *stream[REQ, RES] {
	s, ok := getOpt[*stream[REQ, RES]](q, streamOptKey)
	if !ok {
		panic("stream is not defined on query. this is a bug.")
	}
	return s
}
