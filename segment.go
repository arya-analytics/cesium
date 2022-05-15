package cesium

import (
	"bytes"
	"github.com/arya-analytics/x/binary"
	"github.com/arya-analytics/x/kv"
	"github.com/arya-analytics/x/util/errutil"
	"io"
	"sort"
)

// |||||| SEGMENT ||||||

type Segment struct {
	ChannelKey ChannelKey
	Start      TimeStamp
	Data       []byte
	fileKey    fileKey
	offset     int64
	size       Size
}

// |||||| HEADER ||||||

type segmentHeader struct {
	ChannelPK ChannelKey
	FilePK    fileKey
	Offset    int64
	Start     TimeStamp
	Size      Size
}

func (sg Segment) header() segmentHeader {
	return segmentHeader{
		ChannelPK: sg.ChannelKey,
		FilePK:    sg.fileKey,
		Offset:    sg.offset,
		Start:     sg.Start,
		Size:      sg.size,
	}
}

// Size implements allocate.Item.
func (sg Segment) Size() int {
	if len(sg.Data) == 0 {
		return int(sg.size)
	}
	return len(sg.Data)
}

// Key implements allocate.Item.
func (sg Segment) Key() ChannelKey {
	return sg.ChannelKey
}

// Flush implements kv.Flusher.
func (sg Segment) Flush(w io.Writer) error {
	return binary.Write(w, sg.header())
}

// Load implements kv.Loader.
func (sg *Segment) Load(r io.Reader) error {
	// Unfortunately this is the most efficient way I've found to read the header
	c := errutil.NewCatchRead(r)
	c.Read(&sg.ChannelKey)
	c.Read(&sg.fileKey)
	c.Read(&sg.offset)
	c.Read(&sg.Start)
	c.Read(&sg.size)
	return c.Error()
}

func (sg Segment) flushData(w io.Writer) error {
	return binary.Write(w, sg.Data)
}

const segmentKVPrefix = "cs-sg"

func (sg Segment) KVKey() []byte {
	key, err := kv.CompositeKey(segmentKVPrefix, sg.ChannelKey, sg.Start)
	if err != nil {
		panic(err)
	}
	return key
}

// |||||| KV ||||||

type segmentKV struct {
	kv kv.KV
}

func (sk segmentKV) set(s Segment) error {
	return kv.Flush(sk.kv, s.KVKey(), s)
}

func (sk segmentKV) filter(tr TimeRange, cpk ChannelKey) (segments []Segment, err error) {
	startKey, endKey := generateRangeKeys(cpk, tr)
	iter := sk.kv.IterRange(startKey, endKey)
	defer func() {
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()
	for iter.First(); iter.Valid(); iter.Next() {
		b := new(bytes.Buffer)
		b.Write(iter.Value())
		s := &Segment{}
		if err := s.Load(b); err != nil {
			return nil, err
		}
		segments = append(segments, *s)
	}
	return segments, nil
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

// |||||| CONVERTER ||||||

func (sg Segment) Range(dr DataRate, d DataType) TimeRange {
	return TimeRange{
		Start: sg.Start,
		End:   sg.Start.Add(dr.ByteSpan(len(sg.Data), d)),
	}
}

// |||||| SORT ||||||

func Sort(segments []Segment) {
	sort.Slice(segments, func(i, j int) bool { return segments[i].Start.Before(segments[j].Start) })
}
