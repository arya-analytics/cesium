package cesium

import (
	"cesium/internal/binary"
	"cesium/internal/kv"
	"io"
)

type ChannelKey int16
type fileKey int16

type Channel struct {
	Key      ChannelKey
	DataRate DataRate
	DataType DataType
	Active   bool
}

type LockChannelKVHook struct {
	ckv channelKV
}

func (l LockChannelKVHook) Exec(channels ...Channel) error {
	for _, channel := range channels {
		channel.Active = true
	}
	return l.ckv.setMultiple(channels...)
}

// Flush implements kv.Flusher.
func (c Channel) Flush(w io.Writer) error {
	return binary.Write(w, c)
}

// Load implements kv.Loader.
func (c *Channel) Load(r io.Reader) error {
	return binary.Read(r, &c)
}

const channelKVPrefix = "chan"

func (c Channel) KVKey() []byte {
	return kv.DashedCompositeKey(channelKVPrefix, c.Key)
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
