package caesium

import (
	"caesium/util/binary"
	"caesium/util/errutil"
	"io"
)

// |||||| SEGMENT ||||||

type Segment struct {
	segmentHeader
	segmentData
}

// |||||| HEADER ||||||

type segmentHeader struct {
	ChannelPK PK
	FilePK    PK
	Start     TimeStamp
	Size      Size
}

func (sg segmentHeader) flush(w io.Writer) error {
	cw := errutil.NewCatchWrite(w)
	cw.Write(&sg.ChannelPK)
	cw.Write(&sg.Start)
	cw.Write(&sg.Size)
	return cw.Error()
}

func (sg segmentHeader) fill(r io.Reader) (segmentHeader, error) {
	cr := errutil.NewCatchRead(r)
	cr.Read(&sg.ChannelPK)
	cr.Read(&sg.Start)
	cr.Read(&sg.Size)
	return sg, cr.Error()
}

// |||||| DATA ||||||

type segmentData struct {
	Data []byte
}

func (sg segmentData) flush(w io.Writer) error {
	return binary.Write(w, sg.Data)
}

func (sg segmentData) fill(r io.Reader) (segmentData, error) {
	return sg, binary.Read(r, &sg.Data)
}

// |||||| KV ||||||

type segmentKV struct {
	prefix kvPrefix
	flush  flushKV[segmentHeader]
}

func newSegmentKV(kve kvEngine) segmentKV {
	return segmentKV{
		prefix: kvPrefix{[]byte("seg")},
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
