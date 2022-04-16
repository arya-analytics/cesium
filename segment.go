package caesium

import (
	"caesium/util/binary"
	"caesium/util/errutil"
	"io"
)

// |||||| SEGMENT ||||||

type Segment struct {
	segmentHeader
	Data SegmentData
}

// |||||| HEADER ||||||

type segmentHeader struct {
	ChannelPK PK
	FilePK    PK
	Start     TimeStamp
	Size      Size
}

func (sg segmentHeader) flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(&sg.ChannelPK)
	c.Write(&sg.Start)
	c.Write(&sg.Size)
	return c.Error()
}

func (sg segmentHeader) fill(r io.Reader) (segmentHeader, error) {
	c := errutil.NewCatchRead(r)
	c.Read(&sg.ChannelPK)
	c.Read(&sg.Start)
	c.Read(&sg.Size)
	return sg, c.Error()
}

// |||||| DATA ||||||

type SegmentData []byte

func (sg SegmentData) flush(w io.Writer) error {
	return binary.Write(w, sg)
}

func (sg SegmentData) fill(r io.Reader) (SegmentData, error) {
	return sg, binary.Read(r, sg)
}

// |||||| KV ||||||

type segmentKV struct {
	prefix kvPrefix
	flush  flushKV[segmentHeader]
}

const segmentKVPrefix = "seg"

func newSegmentKV(kve kvEngine) segmentKV {
	return segmentKV{
		prefix: kvPrefix{[]byte(segmentKVPrefix)},
		flush:  flushKV[segmentHeader]{kve},
	}
}

func (sk segmentKV) get(pk PK) (s Segment, error error) {
	h, err := sk.flush.fill(sk.prefix.pk(pk), s.segmentHeader)
	return Segment{segmentHeader: h}, err
}

func (sk segmentKV) set(pk PK, s Segment) error {
	return sk.flush.flush(sk.prefix.pk(pk), s.segmentHeader)
}
