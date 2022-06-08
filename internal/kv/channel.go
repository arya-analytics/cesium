package kv

import (
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/x/gorp"
	"github.com/arya-analytics/x/kv"
)

type Channel struct {
	db *gorp.DB
}

func NewChannel(kve kv.KV) *Channel { return &Channel{db: gorp.Wrap(kve)} }

func (c *Channel) Get(key channel.Key) (res channel.Channel, err error) {
	return res, gorp.NewRetrieve[channel.Channel]().Entry(&res).WhereKeys(key).Exec(c.db)
}

func (c *Channel) Set(ch channel.Channel) error {
	return gorp.NewCreate[channel.Channel]().Entry(&ch).Exec(c.db)
}
