package cesium

import (
	"cesium/util/errutil"
	"context"
	log "github.com/sirupsen/logrus"
	"go/types"
	"io"
)

type runner struct {
	ckv   channelKV
	skv   segmentKV
	pst   Persist
	kve   kvEngine
	queue *tickQueue
}

type operation interface {
	filePK() PK
	exec(f KeyFile)
	sendError(err error)
	context() context.Context
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
		o := opSet{
			done: make(chan struct{}, len(segments)),
			ops:  make([]operation, 0, len(segments)),
		}
		for _, seg := range segments {
			op := retrieveOperation{seg: seg, done: o.done, stream: s, ctx: ctx}
			o.ops = append(o.ops, op)
			r.queue.ops <- op
		}
		o.wait()
		s.res <- RetrieveResponse{Err: io.EOF}
	}()
	return nil
}

type retrieveOperation struct {
	seg    Segment
	stream stream[types.Nil, RetrieveResponse]
	done   chan struct{}
	ctx    context.Context
}

func (ro retrieveOperation) filePK() PK {
	return ro.seg.FilePK
}

func (ro retrieveOperation) context() context.Context {
	return ro.ctx
}

func (ro retrieveOperation) sendError(err error) {
	ro.stream.res <- RetrieveResponse{Err: err}
}

func (ro retrieveOperation) exec(f KeyFile) {
	//log.WithFields(log.Fields{
	//	"file":   f.PK(),
	//	"offset": ro.seg.Offset,
	//}).Info("retrieving segment")
	c := errutil.NewCatchReadWriteSeek(f)
	c.Seek(ro.seg.Offset, io.SeekStart)
	b := make([]byte, ro.seg.Size)
	c.Read(b)
	ro.seg.Data = b
	if c.Error() == io.EOF {
		log.Fatal("unexpected EOF")
	}
	ro.stream.res <- RetrieveResponse{Segments: []Segment{ro.seg}, Err: c.Error()}
	ro.done <- struct{}{}
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
	s.goPipe(ctx, func(req CreateRequest) {
		r.pst.Exec(createOperation{
			ctx: ctx,
			cpk: cpk,
			fpk: fpk,
			seg: req.Segments,
			kv:  r.skv,
		})
	})
	return nil
}

type createOperation struct {
	fpk PK
	cpk PK
	seg []Segment
	kv  segmentKV
	s   stream[CreateRequest, CreateResponse]
	ctx context.Context
}

func (cr createOperation) filePK() PK {
	return cr.fpk
}

func (cr createOperation) sendError(err error) {
	cr.s.res <- CreateResponse{Err: err}
}

func (cr createOperation) exec(f KeyFile) {
	for _, s := range cr.seg {
		c := errutil.NewCatchReadWriteSeek(f)
		s.FilePK = f.PK()
		s.ChannelPK = cr.cpk
		s.Size = s.size()
		s.Offset = c.Seek(0, io.SeekCurrent)
		c.Exec(func() error { return s.flushData(f) })
		c.Exec(func() error { return cr.kv.set(s) })
		if c.Error() != nil {
			if c.Error() == io.EOF {
				panic(io.ErrUnexpectedEOF)
			}
			cr.s.res <- CreateResponse{Err: c.Error()}
		}
	}
}

func (cr createOperation) context() context.Context {
	return cr.ctx
}
