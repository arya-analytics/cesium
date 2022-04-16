package query

import (
	"caesium/pk"
	"caesium/telem"
	"context"
)

type RetrieveResponse struct {
	Segments []Segment
	Error    error
}

func NewRetrieve() Retrieve {
	return Retrieve{}
}

type Retrieve struct {
	Query
}

func (r Retrieve) WhereChannels(pks ...pk.PK) Retrieve {
	setChannelPKs(r.Query, pks...)
	return r
}

func (r Retrieve) WhereTimeRange(tr telem.TimeRange) Retrieve {
	setTimeRange(r.Query, tr)
	return r
}

func (r Retrieve) Stream(ctx context.Context) <-chan RetrieveResponse {
	return nil
}
