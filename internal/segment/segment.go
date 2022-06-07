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

func (h Header) End(dr telem.DataRate, dt telem.DataType) telem.TimeStamp {
	return h.Start.Add(dr.SizeSpan(telem.Size(h.Size), dt))
}

const prefix = 's'

type Key [13]byte

func (k Key) Bytes() []byte { return k[:] }

func NewKey(channelKey channel.Key, stamp telem.TimeStamp) (key Key) {
	key[0] = prefix
	binary.LittleEndian.PutUint64(key[1:9], uint64(stamp))
	binary.LittleEndian.PutUint16(key[9:13], uint16(channelKey))
	return key
}

type Range struct {
	Channel channel.Channel
	Bound   telem.TimeRange
	Headers []Header
}

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
