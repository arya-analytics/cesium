package query

import (
	"caesium/channel"
	"caesium/kv"
	"caesium/pk"
	"caesium/telem"
	"context"
)

func NewCreateChannel(kve kv.Engine) CreateChannel {
	return CreateChannel{
		kve:   kve,
		Query: Query{opts: map[optKey]interface{}{}},
	}
}

type CreateChannel struct {
	Query
	kve kv.Engine
}

func (c CreateChannel) WithDataRate(dr telem.DataRate) CreateChannel {
	setDataRate(c.Query, dr)
	return c
}

func (c CreateChannel) WithDensity(density telem.Density) CreateChannel {
	setDensity(c.Query, density)
	return c
}

func (c CreateChannel) Exec(ctx context.Context) (channel.Channel, error) {
	d, ok := Density(c.Query)
	if !ok {
		panic("density not set")
	}
	dr, ok := DataRate(c.Query)
	if !ok {
		panic("data rate not set")
	}
	ch := channel.Channel{PK: pk.New(), Density: d, DataRate: dr}
	return ch, channel.Set(c.kve, ch)
}

// |||| DENSITY ||||

const densityOptKey optKey = "ds"

func setDensity(q Query, d telem.Density) {
	q.set(densityOptKey, d)
}

func Density(q Query) (telem.Density, bool) {
	d, ok := getOpt[telem.Density](q, densityOptKey)
	return d, ok
}

// |||| DATA RATE ||||

const dataRateOptKey optKey = "dr"

func setDataRate(q Query, dr telem.DataRate) {
	q.set(dataRateOptKey, dr)
}

func DataRate(q Query) (telem.DataRate, bool) {
	dr, ok := getOpt[telem.DataRate](q, dataRateOptKey)
	return dr, ok
}
