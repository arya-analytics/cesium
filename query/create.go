package query

import (
	"caesium/kv"
	"caesium/pk"
	"context"
)

type CreateRequest struct {
	Segments []Segment
}

type CreateResponse struct {
	Error error
}

func NewCreate(kv kv.Engine) Create {
	return Create{}
}

type Create struct {
	Query
}

func (c Create) WhereChannels(pks ...pk.PK) Create {
	setChannelPKs(c.Query, pks...)
	return c
}

func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, error) {
	return nil, nil
}

func (c Create) Errors() <-chan CreateResponse {
	return nil
}
