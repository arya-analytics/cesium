package caesium

import (
	"context"
	"go/types"
	"io"
)

type runner struct {
	ckv channelKV
	skv segmentKV
	pst Persist
	kve kvEngine
}

type operation interface {
	fileKey() PK
	exec(ctx context.Context, f KeyFile) error
}

func (r *runner) exec(ctx context.Context, q query) error {
	return q.switchVariant(ctx, variantOpts{
		CreateChannel:   r.ckv.exec,
		RetrieveChannel: r.ckv.exec,
		Create:          r.create,
		Retrieve:        r.retrieve,
	})
}

func (r *runner) close() error {
	return r.kve.Close()
}

func (r *runner) retrieve(ctx context.Context, q query) error {
	cpk, err := channelPK(q)
	if err != nil {
		return err
	}
	tr := timeRange(q)

	s := getStream[types.Nil, RetrieveResponse](q)

	segments, err := r.skv.filter(tr, cpk)
	if err != nil {
		return err
	}
	go func() {
		for _, seg := range segments {
			errs := r.pst.Exec(ctx, retrieveOperation{seg: &seg})
			s.res <- RetrieveResponse{
				Segments: []Segment{seg},
				Err:      errs[0],
			}
		}
		s.res <- RetrieveResponse{Err: io.EOF}
	}()
	return nil
}

type retrieveOperation struct {
	seg *Segment
}

func (ro retrieveOperation) FileKey() PK {
	return ro.seg.FilePK
}

func (ro retrieveOperation) Exec(ctx context.Context, f KeyFile) (err error) {
	if _, err := f.Seek(ro.seg.Offset, io.SeekStart); err != nil {
		return err
	}
	ro.seg.Data = make([]byte, ro.seg.Size)
	ro.seg.Data, err = ro.seg.Data.fill(f)
	return err
}

func (r *runner) create(ctx context.Context, q query) error {
	// 1. Open write lock on the channel
	cpk, err := channelPK(q)
	if err != nil {
		return err
	}
	if err := r.ckv.lock(cpk); err != nil {
		return err
	}

	// 2. Retrieve the stream
	s := getStream[CreateRequest, CreateResponse](q)

	fpk := NewPK()

	// 3. Fork a goroutine to execute the query
	s.goPipe(ctx, func(req CreateRequest) CreateResponse {
		errs := r.pst.Exec(ctx, createOperation{
			cpk:     cpk,
			fileKey: fpk,
			seg:     req.Segments,
			kv:      r.skv,
		})
		c := CreateResponse{}
		c.Err = errs[0]
		return c
	})
	return nil
}

type createOperation struct {
	fileKey PK
	cpk     PK
	seg     []Segment
	kv      segmentKV
}

func (cr createOperation) FileKey() PK {
	return cr.fileKey
}

func (cr createOperation) Exec(ctx context.Context, f KeyFile) error {
	for _, s := range cr.seg {
		s.FilePK = f.PK()
		s.ChannelPK = cr.cpk
		s.Size = s.Data.Size()
		if err := s.Data.flush(f); err != nil {
			return err
		}
		ret, err := f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		s.Offset = ret
		if err := cr.kv.set(s); err != nil {
			return err
		}
	}
	return nil
}
