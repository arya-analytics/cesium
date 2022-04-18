package cesium

import (
	"cesium/util/binary"
	"context"
	"io"
)

type Channel struct {
	PK       PK
	DataRate DataRate
	Density  DataType
	Active   bool
}

func (c Channel) flush(w io.Writer) error {
	return binary.Write(w, c)
}

func (c Channel) fill(r io.Reader) (Channel, error) {
	return c, binary.Read(r, &c)
}

type channelKV struct {
	flush flushKV[Channel]
}

const channelKVPrefix = "chan"

func newChannelKV(kve kvEngine) channelKV {
	return channelKV{
		flush: flushKV[Channel]{kve},
	}
}

func generateChannelKey(pk PK) ([]byte, error) {
	return generateKey(channelKVPrefix, pk)
}

func (ck channelKV) get(pk PK) (c Channel, err error) {
	key, err := generateChannelKey(pk)
	if err != nil {
		return c, err
	}
	return ck.flush.fill(key, c)
}

func (ck channelKV) set(pk PK, c Channel) error {
	key, err := generateChannelKey(pk)
	if err != nil {
		return err
	}
	return ck.flush.flush(key, c)
}

func (ck channelKV) lock(pk PK) error {
	c, err := ck.get(pk)
	if err != nil {
		return err
	}
	if c.Active {
		return newSimpleError(ErrChannelLock, "channel is already locked")
	}
	c.Active = true
	return ck.set(pk, c)
}

func (ck channelKV) unlock(pk PK) error {
	c, err := ck.get(pk)
	if err != nil {
		return err
	}
	c.Active = false
	return ck.set(pk, c)
}

func (ck channelKV) exec(ctx context.Context, q query) error {
	return q.switchVariant(ctx, variantOpts{
		CreateChannel:   ck.execCreate,
		RetrieveChannel: ck.execRetrieve,
	})
}

func (ck channelKV) execRetrieve(ctx context.Context, q query) error {
	cpk, err := channelPK(q)
	if err != nil {
		return err
	}
	c, err := ck.get(cpk)
	setQueryRecord[Channel](q, c)
	return err
}

func (ck channelKV) execCreate(ctx context.Context, q query) error {
	dr, ok := dataRate(q)
	if !ok {
		return newSimpleError(ErrInvalidQuery, "no data rate provided to create query")
	}
	ds, ok := density(q)
	if !ok {
		return newSimpleError(ErrInvalidQuery, "no density provided to create query")
	}
	c := Channel{PK: NewPK(), DataRate: dr, Density: ds}
	err := ck.set(c.PK, c)
	setQueryRecord[Channel](q, c)
	return err
}
