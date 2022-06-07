package iterator

import (
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/telem"
	"github.com/cockroachdb/pebble"
)

type Header struct {
	kv      kv.Iterator
	pos     telem.TimeRange
	value   *segment.Range
	rng     telem.TimeRange
	decoder binary.Decoder
}

func NewHeader(kve kv.KV, channel channel.Channel, rng telem.TimeRange, decoder binary.Decoder) *Header {
	h := &Header{
		kv: kve.IterRange(
			segment.NewKey(channel.Key, rng.Start).Bytes(),
			segment.NewKey(channel.Key, rng.End).Bytes(),
		),
		decoder: decoder,
		rng:     rng,
		pos:     telem.TimeRangeZero,
	}
	h.resetValue()
	return h
}

func (h *Header) First() bool {
	h.resetValue()
	if !h.kv.First() {
		return false
	}
	h.loadValue()
	h.updatePos(h.value.UnboundedRange())
	h.boundValueToCurrentPos()
	return true
}

func (h *Header) Last() bool {
	h.resetValue()
	if !h.kv.Last() {
		return false
	}
	h.loadValue()
	h.updatePos(h.value.UnboundedRange())
	h.boundValueToCurrentPos()
	return true
}

func (h *Header) Next() bool {
	h.resetValue()
	if !h.kv.Next() {
		return false
	}
	h.loadValue()
	h.updatePos(h.value.UnboundedRange())
	h.boundValueToCurrentPos()
	return true
}

func (h *Header) SeekLT(stamp telem.TimeStamp) bool {
	if !h.rng.ContainsStamp(stamp) {
		h.updatePos(stamp.SpanRange(0))
		return false
	}
	h.resetValue()
	if !h.kv.SeekLT(h.key(stamp).Bytes()) {
		return false
	}
	h.loadValue()
	h.updatePos(telem.TimeRange{Start: h.value.UnboundedRange().Start, End: stamp})
	h.boundValueToCurrentPos()
	return true
}

func (h *Header) SeekGE(stamp telem.TimeStamp) bool {
	if !h.rng.ContainsStamp(stamp) {
		h.updatePos(stamp.SpanRange(0))
		return false
	}
	h.resetValue()
	if !h.kv.SeekGE(h.key(stamp).Bytes()) {
		return false
	}
	h.loadValue()
	h.updatePos(telem.TimeRange{Start: stamp, End: h.value.UnboundedRange().End})
	h.boundValueToCurrentPos()
	return true
}

func (h *Header) NextSpan(span telem.TimeSpan) bool {
	h.resetValue()
	var (
		rng    = h.pos.End.SpanRange(span)
		endKey = h.key(rng.End).Bytes()
	)
	for {
		if state := h.kv.NextWithLimit(endKey); state != pebble.IterValid {
			break
		}
		h.loadValue()
	}
	h.updatePos(rng)
	h.boundValueToCurrentPos()
	return true
}

func (h *Header) NextRange(tr telem.TimeRange) bool {
	if !h.rng.ContainsRange(tr) {
		h.updatePos(tr)
		return false
	}
	h.SeekLT(tr.Start)
	h.NextSpan(tr.Span())
	h.updatePos(tr)
	h.boundValueToCurrentPos()
	return true
}

func (h *Header) Value() *segment.Range { return h.value }

func (h *Header) Position() telem.TimeRange { return h.pos }

func (h *Header) key(stamp telem.TimeStamp) segment.Key {
	return segment.NewKey(h.value.Channel.Key, stamp)
}

func (h *Header) loadValue() { h.addValue(h.kv.Value()) }

func (h *Header) updatePos(rng telem.TimeRange) { h.pos = rng }

func (h *Header) boundValueToCurrentPos() { h.value.Bound = h.pos }

func (h *Header) resetValue() { h.value = new(segment.Range) }

func (h *Header) addValue(b []byte) {
	addHeader := segment.Header{}
	h.decoder.DecodeStatic(b, &addHeader)
	h.value.Headers = append(h.value.Headers, addHeader)
}
