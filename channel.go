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

func (ck channelKV) execRetrieve(ctx context.Context, q query) error {
	cpk, err := channelPK(q)
	if err != nil {
		return err
	}
	c, err := ck.get(cpk)
	setQueryRecord[Channel](q, c)
	return err
}
