package cesium

import (
	"bytes"
	"cesium/util/binary"
	"cesium/util/errutil"
	"fmt"
	log "github.com/sirupsen/logrus"
	"io"
	"time"
)

// |||||| SEGMENT ||||||

type Segment struct {
	ChannelPK PK
	FilePK    PK
	Offset    int64
	Start     TimeStamp
	Size      Size
	Data      []byte
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
		FilePK:    sg.FilePK,
		Offset:    sg.Offset,
		Start:     sg.Start,
		Size:      sg.Size,
	}
}

func (sg Segment) size() Size {
	return Size(len(sg.Data))
}

func (sg Segment) flush(w io.Writer) error {
	return binary.Write(w, sg.header())
}

func (sg Segment) fill(r io.Reader) (Segment, error) {
	// Unfortunately this is the most efficient way to read the header
	c := errutil.NewCatchRead(r)
	c.Read(&sg.ChannelPK)
	c.Read(&sg.FilePK)
	c.Read(&sg.Offset)
	c.Read(&sg.Start)
	c.Read(&sg.Size)
	return sg, c.Error()
}

func (sg Segment) flushData(w io.Writer) error {
	t0 := time.Now()
	err := binary.Write(w, sg.Data)
	log.WithFields(log.Fields{
		"duration": time.Since(t0),
		"size":     len(sg.Data),
	}).Debug("Flushed segment data")
	return err
}

func readSegmentData(r io.Reader, sg Segment) ([]byte, error) {
	t0 := time.Now()
	data := make([]byte, sg.Size)
	c := errutil.NewCatchRead(r)
	c.Read(data)
	log.WithFields(log.Fields{
		"duration": time.Since(t0),
		"size":     len(data),
	}).Debug("Read segment data")
	return data, c.Error()
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
	startKey, err := generateKey(segmentKVPrefix, cpk)
	if err != nil {
		return nil, err
	}
	endKey, err := generateKey(segmentKVPrefix, cpk, tr.End)
	iter := sk.flush.kvEngine.IterRange(startKey, endKey)
	defer func(iter kvIterator) {
		if err := iter.Close(); err != nil {
			log.Errorf("Error closing iterator: %s", err)
		}
	}(iter)
	for iter.First(); iter.Valid(); iter.Next() {
		b := new(bytes.Buffer)
		b.Write(iter.Value())
		s, err := Segment{}.fill(b)
		if err != nil {
			return nil, err
		}
		segments = append(segments, s)
	}
	if err := iter.Close(); err != nil {
		panic(err)
	}
	return segments, nil
}
