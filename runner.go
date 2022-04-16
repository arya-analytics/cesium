package caesium

import (
	"context"
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
	})
}

func (r *runner) close() error {
	return r.kve.Close()
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
	s.pipe(ctx, func(req CreateRequest) CreateResponse {
		errs := r.pst.Exec(ctx, createOperation{
			fileKey: fpk,
			seg:     req.Segments,
		})
		return CreateResponse{Err: errs[0]}
	})
	return nil
}

type createOperation struct {
	fileKey PK
	seg     []Segment
	kv      segmentKV
}

func (cr createOperation) FileKey() PK {
	return cr.fileKey
}

func (cr createOperation) Exec(ctx context.Context, f KeyFile) error {
	for _, s := range cr.seg {
		pk := NewPK()
		if err := cr.kv.set(pk, s); err != nil {
			return err
		}
		if err := s.Data.flush(f); err != nil {
			return err
		}
	}
	return nil
}
