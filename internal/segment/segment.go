package segment

import (
	"encoding/binary"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/core"
	xbinary "github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/telem"
)

type Segment struct {
	ChannelKey channel.Key
	Start      telem.TimeStamp
	Data       []byte
}

type Header struct {
	ChannelKey channel.Key
	Start      telem.TimeStamp
	FileKey    core.FileKey
	Offset     uint64
	Size       uint64
}

func (h Header) Key() Key { return NewKey(h.ChannelKey, h.Start) }

// GorpKey implements the gorp.Entry interface.
func (h Header) GorpKey() interface{} { return h.Key().Bytes() }

// SetOptions implements the gorp.Entry interface.
func (h Header) SetOptions() (opts []interface{}) { return opts }

func (h Header) End(dr telem.DataRate, dt telem.DataType) telem.TimeStamp {
	return h.Start.Add(dr.SizeSpan(telem.Size(h.Size), dt))
}

const prefix = 's'

type Key [13]byte

func (k Key) Bytes() []byte { return k[:] }

func NewKeyPrefix(channelKey channel.Key) []byte {
	keyPrefix := make([]byte, 6)
	keyPrefix[0] = prefix
	binary.BigEndian.PutUint16(keyPrefix[1:], uint16(channelKey))
	return keyPrefix
}

func NewKey(channelKey channel.Key, stamp telem.TimeStamp) (key Key) {
	key[0] = prefix
	binary.BigEndian.PutUint16(key[1:5], uint16(channelKey))
	binary.BigEndian.PutUint64(key[5:13], uint64(stamp))
	return key
}

type Range struct {
	Channel channel.Channel
	Bound   telem.TimeRange
	Headers []Header
}

func (r *Range) Range() telem.TimeRange { return r.UnboundedRange().BoundBy(r.Bound) }

func (r *Range) Empty() bool { return r.UnboundedRange().IsZero() }

func (r *Range) UnboundedRange() telem.TimeRange {
	if len(r.Headers) == 0 {
		return telem.TimeRangeZero
	}
	return telem.TimeRange{
		Start: r.Headers[0].Start,
		End:   r.Headers[len(r.Headers)-1].End(r.Channel.DataRate, r.Channel.DataType),
	}
}

type HeaderWriter struct {
	KV      kv.Writer
	Encoder xbinary.Encoder
}

func (hw *HeaderWriter) Write(h Header) error {
	return hw.KV.Set(h.Key().Bytes(), hw.Encoder.EncodeStatic(h))
}
