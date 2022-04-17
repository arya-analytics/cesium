package caesium

import (
	"caesium/util/binary"
	"caesium/util/errutil"
	"io"
)

// |||||| SEGMENT ||||||

type Segment struct {
	ChannelPK PK
	FilePK    PK
	Start     TimeStamp
	Size      Size
	Data      SegmentData
}

// |||||| HEADER ||||||

func (sg Segment) flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(&sg.ChannelPK)
	c.Write(&sg.Start)
	c.Write(&sg.Size)
	return c.Error()
}

func (sg Segment) fill(r io.Reader) (Segment, error) {
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
	flush  flushKV[Segment]
}

const segmentKVPrefix = "seg"

func newSegmentKV(kve kvEngine) segmentKV {
	return segmentKV{
		prefix: kvPrefix{[]byte(segmentKVPrefix)},
		flush:  flushKV[Segment]{kve},
	}
}

func (sk segmentKV) get(pk PK) (s Segment, error error) {
	return sk.flush.fill(sk.prefix.pk(pk), s)
}

func (sk segmentKV) set(pk PK, s Segment) error {
	return sk.flush.flush(sk.prefix.pk(pk), s)
}
