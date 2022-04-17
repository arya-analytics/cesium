package caesium

import (
	"bytes"
	"caesium/util/binary"
	"caesium/util/errutil"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
)

// |||||| SEGMENT ||||||

type Segment struct {
	ChannelPK PK
	FilePK    PK
	Offset    int64
	Start     TimeStamp
	Size      Size
	Data      SegmentData
}

// |||||| HEADER ||||||

func (sg Segment) flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(&sg.ChannelPK)
	c.Write(&sg.FilePK)
	c.Write(&sg.Offset)
	c.Write(&sg.Start)
	c.Write(&sg.Size)
	return c.Error()
}

func (sg Segment) fill(r io.Reader) (Segment, error) {
	c := errutil.NewCatchRead(r)
	c.Read(&sg.ChannelPK)
	c.Read(&sg.FilePK)
	c.Read(&sg.Offset)
	c.Read(&sg.Start)
	c.Read(&sg.Size)
	return sg, c.Error()
}

func (sg Segment) String() string {
	return fmt.Sprintf(`
		ChannelPK: %s
		FilePK: %s
		Offset: %d
		Start: %s
		Size: %s
		Data: %s
	`, sg.ChannelPK, sg.FilePK, sg.Offset, sg.Start, sg.Size, sg.Data)
}

// |||||| DATA ||||||

type SegmentData []byte

func (sg SegmentData) flush(w io.Writer) error {
	return binary.Write(w, sg)
}

func (sg SegmentData) fill(r io.Reader) (SegmentData, error) {
	return sg, binary.Read(r, sg)
}

func (sg SegmentData) Size() Size {
	return Size(len(sg))
}

// |||||| KV ||||||

const segmentKVPrefix = "seg"

type segmentKV struct {
	flush flushKV[Segment]
}

func newSegmentKV(kve kvEngine) segmentKV {
	return segmentKV{flush: flushKV[Segment]{kve}}
}

func generateSegmentKey(s Segment) ([]byte, error) {
	return generateKey(segmentKVPrefix, s.ChannelPK, s.Start)
}

func (sk segmentKV) set(s Segment) error {
	key, err := generateSegmentKey(s)
	if err != nil {
		return err
	}
	return sk.flush.flush(key, s)
}

func (sk segmentKV) filter(tr TimeRange, cpk PK) (segments []Segment, err error) {
	p, err := generateKey(segmentKVPrefix, cpk)
	if err != nil {
		return nil, err
	}
	iter := sk.flush.kvEngine.IterPrefix(p)
	defer func(iter kvIterator) {
		err := iter.Close()
		if err != nil {
			panic(err)
		}
	}(iter)
	for iter.First(); iter.Valid(); iter.Next() {
		s := Segment{}
		b := new(bytes.Buffer)
		b.Write(iter.Value())
		s, err := s.fill(b)
		if err != nil {
			return nil, err
		}
		if s.Start >= tr.Start && s.Start < tr.End {
			segments = append(segments, s)
		}
		log.Info(s)
	}
	return segments, nil
}
