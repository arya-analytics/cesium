package query

import (
	"caesium/pk"
	"caesium/telem"
	"context"
)

func NewDelete() Delete {
	return Delete{}
}

type Delete struct {
	Query
}

func (d Delete) WhereChannels(pks ...pk.PK) Delete {
	setChannelPKs(d.Query, pks...)
	return d
}

func (d Delete) WhereTimeRange(tr telem.TimeRange) Delete {
	return d
}
func (d Delete) Exec(ctx context.Context) error {
	return nil
}
