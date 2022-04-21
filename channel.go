package cesium

import (
	"cesium/util/binary"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
)

type Channel struct {
	PK       PK
	DataRate DataRate
	DataType DataType
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

func generateChannelKey(pk PK) []byte {
	return generateKey(channelKVPrefix, pk)
}

func (ck channelKV) get(pk PK) (c Channel, err error) {
	key := generateChannelKey(pk)
	return ck.flush.fill(key, c)
}

func (ck channelKV) getMultiple(pks ...PK) (cs []Channel, err error) {
	for _, pk := range pks {
		c, err := ck.get(pk)
		if err != nil {
			return cs, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

func (ck channelKV) set(pk PK, c Channel) error {
	key := generateChannelKey(pk)
	return ck.flush.flush(key, c)
}

func (ck channelKV) lock(pks ...PK) error {
	channels, err := ck.getEach(pks...)
	if err != nil {
		return err
	}
	// First we verify that all the channels are inactive.
	if err := ck.verifyInactive(channels...); err != nil {
		return err
	}
	// Then we acquire a lock on each of the channels.
	return ck.setActive(channels...)
}

func (ck channelKV) getEach(pks ...PK) (cs []Channel, err error) {
	for _, pk := range pks {
		c, err := ck.get(pk)
		if err != nil {
			return cs, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

func (ck channelKV) setActive(channels ...Channel) error {
	var locked []PK
	defer func() {
		if err := ck.unlock(locked...); err != nil {
			log.Errorf("[CHKV] failed to unlock channels: %v", err)
		}
	}()
	for _, c := range channels {
		c.Active = true
		if err := ck.set(c.PK, c); err != nil {
			return err
		}
		locked = append(locked, c.PK)
	}
	return nil
}

func (ck channelKV) verifyInactive(channels ...Channel) error {
	for _, ch := range channels {
		if ch.Active {
			return newSimpleError(
				ErrChannelLock,
				fmt.Sprintf("channel with PK %s is locked", ch.PK),
			)
		}
	}
	return nil
}

func (ck channelKV) unlock(pk ...PK) error {
	for _, p := range pk {
		c, err := ck.get(p)
		if err != nil {
			return err
		}
		c.Active = false
		if err := ck.set(p, c); err != nil {
			return err
		}
	}
	return nil
}
