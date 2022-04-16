package query

import (
	"caesium/channel"
	"caesium/kv"
	"caesium/pk"
	"context"
)

func NewRetrieveChannel(kve kv.Engine) RetrieveChannel {
	return RetrieveChannel{
		kve: kve,
		Query: Query{
			opts: map[optKey]interface{}{},
		},
	}
}

type RetrieveChannel struct {
	kve kv.Engine
	Query
}

func (r RetrieveChannel) WherePK(pk pk.PK) RetrieveChannel {
	setChannelPKs(r.Query, pk)
	return r
}

func (r RetrieveChannel) Exec(ctx context.Context) (channel.Channel, error) {
	pks := ChannelPKs(r.Query)
	if len(pks) == 0 {
		panic("no channel pks")
	}
	return channel.Get(r.kve, pks[0])
}
