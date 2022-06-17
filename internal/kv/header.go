package kv

import (
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
)

type Header struct {
	db *gorp.DB
}

func NewHeader(kve kv.KV) *Header { return &Header{db: gorp.Wrap(kve, gorp.WithoutTypePrefix())} }

func (s *Header) Get(key segment.Key) (res segment.Header, err error) {
	return res, gorp.NewRetrieve[[]byte, segment.Header]().Entry(&res).WhereKeys(key.Bytes()).Exec(s.db)
}

func (s *Header) Set(header segment.Header) error {
	return gorp.NewCreate[[]byte, segment.Header]().Entry(&header).Exec(s.db)
}

func (s *Header) SetMultiple(headers []segment.Header) error {
	return gorp.NewCreate[[]byte, segment.Header]().Entries(&headers).Exec(s.db)
}
