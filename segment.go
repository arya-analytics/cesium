package cesium

import (
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/kv"
	"io"
	"sort"
)

// |||||| CORE |||||||

type Segment struct {
	ChannelKey ChannelKey
	Start      TimeStamp
	Data       []byte
	fileKey    fileKey
	offset     int64
	size       Size
}

type SegmentHeader struct {
	ChannelKey ChannelKey
	Start      TimeStamp
	FileKey    fileKey
	Offset     int64
	Size       Size
}

func (sg Segment) Header() SegmentHeader {
	return SegmentHeader{ChannelKey: sg.ChannelKey, Start: sg.Start, FileKey: sg.fileKey, Offset: sg.offset, Size: sg.size}
}

func (sg SegmentHeader) Flush(w io.Writer) error { return binary.Flush(w, sg) }

func (sg *Segment) LoadHeader(header SegmentHeader) {
	sg.ChannelKey = header.ChannelKey
	sg.Start = header.Start
	sg.fileKey = header.FileKey
	sg.offset = header.Offset
	sg.size = header.Size
}

// Size returns the Size of the segment in bytes.
func (sg Segment) Size() int {
	l := len(sg.Data)
	if l == 0 {
		return int(sg.size)
	}
	return l
}

// Key implements allocate.Item.
func (sg Segment) Key() ChannelKey { return sg.ChannelKey }

const segmentKVPrefix = "cs/sg"

func (sg Segment) KVKey() []byte {
	key := kv.StaticCompositeKey(segmentKVPrefix, sg.ChannelKey, sg.Start)
	return key
}

func (sg Segment) flushData(w io.Writer) error { return binary.Write(w, sg.Data) }

// |||||| SUGARED ||||||

func Sort(segments []Segment) {
	sort.Slice(segments, func(i, j int) bool { return segments[i].Start.Before(segments[j].Start) })
}

type segmentKV struct {
	kv kv.KV
}

func (sk segmentKV) set(s Segment) error {
	return kv.Flush(sk.kv, s.KVKey(), s.Header())
}

func generateRangeKeys(cpk ChannelKey, tr TimeRange) ([]byte, []byte) {
	s, err := kv.CompositeKey(segmentKVPrefix, cpk, tr.Start)
	if err != nil {
		panic(err)
	}
	e, err := kv.CompositeKey(segmentKVPrefix, cpk, tr.End)
	if err != nil {
		panic(err)
	}
	return s, e
}
