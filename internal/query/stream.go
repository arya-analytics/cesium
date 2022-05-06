package query

type Response interface {
	Error() error
}

type Request interface{}

type Stream[REQ Request, RES Response] struct {
	Requests  chan REQ
	Responses chan RES
}

func (s Stream[REQ, RES]) CloseResponses() {
	close(s.Responses)
}

const streamOptKey OptionKey = "stream"

func SetStream[REQ Request, RES Response](q Query, s Stream[REQ, RES]) {
	q.SetOnce(streamOptKey, s)
}

func GetStream[REQ Request, RES Response](q Query) Stream[REQ, RES] {
	return q.GetRequired(streamOptKey).(Stream[REQ, RES])
}

type CloseStreamResponseHook[REQ Request, RES Response] struct{}

func (h CloseStreamResponseHook[REQ, RES]) Exec(q Query) error {
	GetStream[REQ, RES](q).CloseResponses()
	return nil
}
