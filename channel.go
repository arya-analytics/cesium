package cesium

import (
	"cesium/internal/binary"
	"cesium/internal/kv"
	"context"
	"io"
)

type ChannelKey = int16

type Channel struct {
	Key      ChannelKey
	DataRate DataRate
	DataType DataType
	Active   bool
}

func ChannelKeys(channels []Channel) []ChannelKey {
	keys := make([]ChannelKey, len(channels))
	for i, channel := range channels {
		keys[i] = channel.Key
	}
	return keys
}

// |||||| KV ||||||

// Flush implements kv.Flusher.
func (c Channel) Flush(w io.Writer) error {
	return binary.Write(w, c)
}

// Load implements kv.Loader.
func (c *Channel) Load(r io.Reader) error {
	return binary.Read(r, c)
}

const channelKVPrefix = "chan"

func (c Channel) KVKey() []byte {
	return kv.CompositeKey(channelKVPrefix, c.Key)
}

// LKey implements lock.MapItem.
func (c Channel) LKey() ChannelKey {
	return c.Key
}

type channelKV struct {
	kv kv.KV
}

func (ckv channelKV) get(key ChannelKey) (c Channel, err error) {
	k := Channel{Key: key}.KVKey()
	b, err := ckv.kv.Get(k)
	if err != nil {
		return c, err
	}
	return c, kv.LoadBytes(b, &c)
}

func (ckv channelKV) getMultiple(keys ...ChannelKey) (cs []Channel, err error) {
	for _, pk := range keys {
		c, err := ckv.get(pk)
		if err != nil {
			return cs, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

func (ckv channelKV) setMultiple(cs ...Channel) error {
	for _, c := range cs {
		if err := ckv.set(c); err != nil {
			return err
		}
	}
	return nil
}

func (ckv channelKV) set(c Channel) error {
	return kv.Flush(ckv.kv, c.KVKey(), c)
}

func (ckv channelKV) getEach(keys ...ChannelKey) (cs []Channel, err error) {
	for _, pk := range keys {
		c, err := ckv.get(pk)
		if err != nil {
			return cs, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

// |||||| CREATE ||||||

type createChannelQueryExecutor struct {
	ckv     channelKV
	counter *kv.PersistedCounter
}

func (cr *createChannelQueryExecutor) exec(_ context.Context, q query) (err error) {
	dr, ok := dataRate(q)
	if !ok {
		return newSimpleError(ErrInvalidQuery, "no data rate provided to create query")
	}
	ds, ok := density(q)
	if !ok {
		return newSimpleError(ErrInvalidQuery, "no density provided to create query")
	}
	npk, err := cr.counter.Increment()
	if err != nil {
		return err
	}
	c := Channel{Key: ChannelKey(npk), DataRate: dr, DataType: ds}
	err = cr.ckv.set(c)
	setQueryRecord[Channel](q, c)
	return err
}

// |||||| RETRIEVE ||||||

type retrieveChannelQueryExecutor struct {
	ckv channelKV
}

func (rc *retrieveChannelQueryExecutor) exec(_ context.Context, q query) error {
	_, ok := q.variant.(RetrieveChannel)
	if !ok {
		return nil
	}
	cpk, err := getChannelKey(q)
	if err != nil {
		return err
	}
	c, err := rc.ckv.get(cpk)
	setQueryRecord[Channel](q, c)
	return err
}
