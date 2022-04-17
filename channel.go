package caesium

import (
	"caesium/util/errutil"
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
	cw := errutil.NewCatchWrite(w)
	cw.Write(c.PK)
	cw.Write(c.DataRate)
	cw.Write(c.Density)
	return cw.Error()
}

func (c Channel) fill(r io.Reader) (Channel, error) {
	cr := errutil.NewCatchRead(r)
	cr.Read(&c.PK)
	cr.Read(&c.DataRate)
	cr.Read(&c.Density)
	return c, cr.Error()
}

type channelKV struct {
	prefix kvPrefix
	flush  flushKV[Channel]
}

const channelKVPrefix = "chan"

func newChannelKV(kve kvEngine) channelKV {
	return channelKV{
		prefix: kvPrefix{[]byte(channelKVPrefix)},
		flush:  flushKV[Channel]{kve},
	}
}

func (ck channelKV) get(pk PK) (c Channel, err error) {
	return ck.flush.fill(ck.prefix.pk(pk), c)
}

func (ck channelKV) set(pk PK, c Channel) error {
	return ck.flush.flush(ck.prefix.pk(pk), c)
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
