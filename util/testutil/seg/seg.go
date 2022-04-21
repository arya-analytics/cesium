package seg

import (
	"cesium"
	"cesium/util/binary"
	"io"
	"math/rand"
)

type DataFactory func(n int) []byte

func New(c cesium.Channel, fac DataFactory, start cesium.TimeStamp, span cesium.TimeSpan) cesium.Segment {
	return cesium.Segment{
		ChannelPK: c.PK,
		Data:      generateSpan(c, fac, span),
		Start:     start,
	}
}

func NewSet(c cesium.Channel, fac DataFactory, start cesium.TimeStamp, span cesium.TimeSpan, n int) []cesium.Segment {
	s := make([]cesium.Segment, n)
	for i := 0; i < n; i++ {
		s[i] = New(c, fac, start, span)
		start = start.Add(span)
	}
	return s
}

// |||| FLOAT 64 |||||

func GenSlice[T any](n int, fac func(int) T) []T {
	s := make([]T, n)
	for i := 0; i < n; i++ {
		s[i] = fac(i)
	}
	return s

}

func sequentialFloat64Slice(n int) []float64 {
	return GenSlice[float64](n, func(i int) float64 { return float64(i) })
}

func randomFloat64(n int) []float64 {
	return GenSlice[float64](n, func(i int) float64 { return rand.Float64() })
}

func SeqFloat64(n int) []byte {
	return writeBytes(sequentialFloat64Slice(n))
}

func RandFloat64(n int) []byte {
	return writeBytes(randomFloat64(n))
}

func writeBytes(data interface{}) []byte {
	b, err := binary.Marshal(data)
	if err != nil {
		panic(err)
	}
	return b
}

func generateSpan(c cesium.Channel, fac DataFactory, span cesium.TimeSpan) []byte {
	sc := c.DataRate.SampleCount(span)
	return fac(sc)
}

func DataTypeFactory(dt cesium.DataType) DataFactory {
	m := map[cesium.DataType]DataFactory{
		cesium.Float64: SeqFloat64,
	}
	return m[dt]
}

// |||||| SEQUENTIAL FACTORY ||||||

type SequentialFactory struct {
	FirstTS cesium.TimeStamp
	PrevTS  cesium.TimeStamp
	Factory DataFactory
	Span    cesium.TimeSpan
	Channel cesium.Channel
}

func NewSequentialFactory(c cesium.Channel, fac DataFactory, span cesium.TimeSpan) *SequentialFactory {
	return &SequentialFactory{FirstTS: 0, PrevTS: 0, Factory: fac, Span: span, Channel: c}
}

func (sf *SequentialFactory) Next() cesium.Segment {
	s := New(sf.Channel, sf.Factory, sf.PrevTS, sf.Span)
	sf.PrevTS = sf.PrevTS.Add(sf.Span)
	return s
}

func (sf *SequentialFactory) NextN(n int) []cesium.Segment {
	s := NewSet(sf.Channel, sf.Factory, sf.PrevTS, sf.Span, n)
	sf.PrevTS = s[n-1].Start.Add(sf.Span)
	return s
}

// ||||| STREAM CREATE ||||||

type StreamCreate struct {
	*SequentialFactory
	Req chan<- cesium.CreateRequest
	Res <-chan cesium.CreateResponse
}

func (sc *StreamCreate) Create() cesium.CreateRequest {
	req := cesium.CreateRequest{Segments: []cesium.Segment{sc.Next()}}
	sc.Req <- req
	return req
}

func (sc *StreamCreate) CreateN(n int) cesium.CreateRequest {
	req := cesium.CreateRequest{Segments: sc.NextN(n)}
	sc.Req <- req
	return req
}

func (sc *StreamCreate) CreateCRequestsOfN(c, n int) []cesium.CreateRequest {
	reqs := make([]cesium.CreateRequest, c)
	for i := 0; i < c; i++ {
		req := sc.CreateN(n)
		sc.Req <- req
		reqs[i] = req
	}
	return reqs
}

func (sc *StreamCreate) CloseAndWait() error {
	close(sc.Req)
	for resV := range sc.Res {
		if resV.Err == io.EOF {
			return nil
		}
		return resV.Err
	}
	return nil
}

// |||||| STREAM RETRIEVE ||||||

type StreamRetrieve struct {
	Res <-chan cesium.RetrieveResponse
}

func (sr StreamRetrieve) All() ([]cesium.Segment, error) {
	var s []cesium.Segment
	for resV := range sr.Res {
		if resV.Err == io.EOF {
			return s, nil
		}
		if resV.Err != nil {
			return nil, resV.Err
		}
		s = append(s, resV.Segments...)
	}
	return s, nil
}
