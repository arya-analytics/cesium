package cesium

import (
	"bytes"
	"cesium/util/binary"
	"cesium/util/errutil"
	"fmt"
	"io"
)

// |||||| SEGMENT ||||||

type Segment struct {
	ChannelPK PK
	Start     TimeStamp
	Data      []byte
	filePk    PK
	offset    int64
	size      Size
}

// |||||| HEADER ||||||

type segmentHeader struct {
	ChannelPK PK
	FilePK    PK
	Offset    int64
	Start     TimeStamp
	Size      Size
}

func (sg Segment) header() segmentHeader {
	return segmentHeader{
		ChannelPK: sg.ChannelPK,
		FilePK:    sg.filePk,
		Offset:    sg.offset,
		Start:     sg.Start,
		Size:      sg.size,
	}
}

func (sg Segment) PKV() PK {
	return sg.ChannelPK
}

func (sg Segment) Size() Size {
	if len(sg.Data) == 0 {
		return sg.size
	}
	return Size(len(sg.Data))
}

func (sg Segment) flush(w io.Writer) error {
	return binary.Write(w, sg.header())
}

func (sg Segment) fill(r io.Reader) (Segment, error) {
	// Unfortunately this is the most efficient way to read the header
	c := errutil.NewCatchRead(r)
	c.Read(&sg.ChannelPK)
	c.Read(&sg.filePk)
	c.Read(&sg.offset)
	c.Read(&sg.Start)
	c.Read(&sg.size)
	return sg, c.Error()
}

func (sg Segment) flushData(w io.Writer) error {
	err := binary.Write(w, sg.Data)
	return err
}

func (sg Segment) String() string {
	return fmt.Sprintf(`
		ChannelPK: %s
		filePk: %s
		offset: %d
		Start: %s
		size: %s
		Data: %s
	`, sg.ChannelPK, sg.filePk, sg.offset, sg.Start, sg.size, sg.Data)
}

// |||||| KV ||||||

const segmentKVPrefix = "seg"

type segmentKV struct {
	flushKV flushKV[Segment]
}

func newSegmentKV(kve kvEngine) segmentKV {
	return segmentKV{flushKV: flushKV[Segment]{kve}}
}

func generateSegmentKey(s Segment) []byte {
	return generateKey(segmentKVPrefix, s.ChannelPK, s.Start)
}

func (sk segmentKV) set(s Segment) error {
	key := generateSegmentKey(s)
	return sk.flushKV.flush(key, s)
}

func (sk segmentKV) latest(cPK PK) (Segment, error) {
	key := generateKey(segmentKVPrefix, cPK)
	iter := sk.flushKV.kvEngine.IterPrefix(key)
	defer func() {
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()
	if ok := iter.Last(); !ok {
		return Segment{}, newSimpleError(ErrNotFound, "No segments found")
	}
	b := new(bytes.Buffer)
	b.Write(iter.Value())
	s, err := Segment{}.fill(b)
	return s, err
}

func (sk segmentKV) filter(tr TimeRange, cpk PK) (segments []Segment, err error) {
	startKey, endKey := generateRangeKeys(cpk, tr)
	iter := sk.flushKV.kvEngine.IterRange(startKey, endKey)
	defer func() {
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()
	for iter.First(); iter.Valid(); iter.Next() {
		b := new(bytes.Buffer)
		b.Write(iter.Value())
		s, err := Segment{}.fill(b)
		if err != nil {
			return nil, err
		}
		segments = append(segments, s)
	}
	return segments, nil
}

func generateRangeKeys(cpk PK, tr TimeRange) ([]byte, []byte) {
	startKey := generateKey(segmentKVPrefix, cpk, tr.Start)
	endKey := generateKey(segmentKVPrefix, cpk, tr.End)
	return startKey, endKey
}

// |||||| CONVERTER ||||||

func (sg Segment) ToFloat64() []float64 {
	return binary.ToFloat64(sg.Data)
}

func (sg Segment) Range(dr DataRate, d DataType) TimeRange {
	return TimeRange{
		Start: sg.Start,
		End:   sg.Start.Add(dr.ByteSpan(len(sg.Data), d)),
	}
}
