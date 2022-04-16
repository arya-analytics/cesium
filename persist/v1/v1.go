package v1

import (
	"caesium/persist"
	"caesium/persist/keyfs"
	"caesium/pk"
	"caesium/telem"
	"caesium/util/errutil"
	"context"
	"io"
)

type Segment struct {
	ChannelPK pk.PK
	Size      uint64
	StartTS   telem.TimeStamp
	Data      []byte
}

func (s Segment) flush(w io.Writer) error {
	c := errutil.NewCatchWrite(w)
	c.Write(s.ChannelPK)
	c.Write(s.Size)
	c.Write(s.StartTS)
	c.Write(s.Data)
	return c.Error()
}

func (s *Segment) ReadHeader(r io.Reader) error {
	c := errutil.NewCatchRead(r)
	c.Read(&s.ChannelPK)
	c.Read(&s.Size)
	c.Read(&s.StartTS)
	return c.Error()
}

func (s *Segment) ReadData(r io.Reader) error {
	c := errutil.NewCatchRead(r)
	//t := time.Now()
	s.Data = make([]byte, s.Size)
	c.Read(s.Data)
	return c.Error()
}

//func readSegmentHeader(r io.Reader) (s Segment, err error) {
//	c := errutil.NewCatchRead(r)
//	c.Read(&s.ChannelPK)
//	c.Read(&s.Size)
//	c.Read(&s.StartTS)
//	c.Read(&s.Data)
//	return s, c.Error()
//}
//

func readSegmentData(size uint64, r io.Reader) ([]byte, error) {
	data := make([]byte, size)
	return data, persist.Read(r, &data)
}

type Retrieve struct {
	FilePK    pk.PK
	ChannelPK pk.PK
	TR        telem.TimeRange
	Results   []Segment
}

func (r *Retrieve) FileKey() pk.PK {
	return r.FilePK
}

func (r *Retrieve) Exec(ctx context.Context, f keyfs.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for {
		s := &Segment{}
		err := s.ReadHeader(f)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err = s.ReadData(f); err != nil {
			return err
		}
		r.Results = append(r.Results, *s)
	}
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	return nil
}

type Create struct {
	FilePK   pk.PK
	Segments []Segment
}

func (c *Create) FileKey() pk.PK {
	return c.FilePK
}

func (c *Create) Exec(ctx context.Context, f keyfs.File) error {
	for _, segment := range c.Segments {
		if err := segment.flush(f); err != nil {
			return err
		}
	}
	return nil
}
