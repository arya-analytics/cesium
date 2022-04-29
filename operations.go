package cesium

import (
	"cesium/internal/allocate"
	"cesium/kfs"
	"cesium/util/errutil"
	"cesium/util/wg"
	"context"
	"fmt"
	"go/types"
	"io"
)

type file = kfs.File[fileKey]

type segmentAllocator = allocate.Allocator[ChannelKey, fileKey, Segment]

// |||||| RETRIEVE ||||||

// || PARSE ||

type retrieveParse struct {
	ckv channelKV
	skv segmentKV
}

func (rp retrieveParse) parse(ctx context.Context, q query) (w wg.Slice[retrieveOperation], err error) {
	cPKs, err := channelKeys(q, true)
	if err != nil {
		return w, err
	}
	if _, err := rp.ckv.getMultiple(cPKs...); err != nil {
		return w, err
	}
	tr := timeRange(q)
	var segments []Segment
	for _, cpk := range cPKs {
		nSegments, err := rp.skv.filter(tr, cpk)
		if err != nil {
			return w, err
		}
		if len(nSegments) == 0 {
			return w, newSimpleError(ErrNotFound, "no segments found to satisfy query")
		}
		segments = append(segments, nSegments...)
	}
	w.Done = make(chan struct{}, len(segments))
	s := getStream[types.Nil, RetrieveResponse](q)
	for _, seg := range segments {
		w.Items = append(w.Items, retrieveOperation{seg: seg, done: w.Done, ctx: ctx, stream: s})
	}
	return w, nil
}

// || OP ||

type retrieveOperation struct {
	seg    Segment
	stream *stream[types.Nil, RetrieveResponse]
	done   chan struct{}
	ctx    context.Context
}

func (ro retrieveOperation) FileKey() fileKey {
	return ro.seg.fileKey
}

func (ro retrieveOperation) context() context.Context {
	return ro.ctx
}

func (ro retrieveOperation) sendError(err error) {
	ro.stream.res <- RetrieveResponse{Err: err}
}

func (ro retrieveOperation) exec(f file) {
	c := errutil.NewCatchReadWriteSeek(f)
	c.Seek(ro.seg.offset, io.SeekStart)
	b := make([]byte, ro.seg.size)
	c.Read(b)
	ro.seg.Data = b
	if c.Error() == io.EOF {
		panic("get encountered unexpected EOF. this is a bug.")
	}
	ro.stream.res <- RetrieveResponse{Segments: []Segment{ro.seg}, Err: c.Error()}
	ro.done <- struct{}{}
}

func (ro retrieveOperation) offset() int64 {
	return ro.seg.offset
}

func (ro retrieveOperation) String() string {
	return fmt.Sprintf(
		"[OP] Retrieve - Channel %stream | File %stream | offset %d | size %d",
		ro.seg.ChannelKey,
		ro.FileKey(),
		ro.offset(),
		ro.seg.size,
	)
}

// |||||| CREATE ||||||

type createParse struct {
	skv       segmentKV
	allocator segmentAllocator
	stream    *stream[CreateRequest, CreateResponse]
}

func (cp createParse) parse(ctx context.Context, req CreateRequest) (w wg.Slice[createOperation], err error) {
	w.Done = make(chan struct{}, len(req.Segments))
	alloc := cp.allocator.Allocate(req.Segments...)
	for i, seg := range req.Segments {
		w.Items = append(w.Items, createOperation{
			ctx:        ctx,
			fileKey:    alloc[i],
			channelKey: seg.ChannelKey,
			segments:   cp.skv,
			stream:     cp.stream,
			seg:        req.Segments,
			done:       w.Done,
		})
	}
	return w, nil
}

type createOperation struct {
	fileKey    fileKey
	channelKey ChannelKey
	seg        []Segment
	segments   segmentKV
	stream     *stream[CreateRequest, CreateResponse]
	ctx        context.Context
	done       chan struct{}
}

func (cr createOperation) FileKey() fileKey {
	return cr.fileKey
}

func (cr createOperation) SendError(err error) {
	cr.stream.res <- CreateResponse{Err: err}
}

func (cr createOperation) Exec(f file) {
	c := errutil.NewCatchReadWriteSeek(f, errutil.WithHooks(cr.SendError))
	// TODO: check if this is actually necessary.
	c.Seek(0, io.SeekEnd)
	for _, s := range cr.seg {
		s.fileKey = f.Key()
		s.ChannelKey = cr.channelKey
		s.size = Size(s.Size())
		s.offset = c.Seek(0, io.SeekCurrent)
		c.Exec(func() error { return s.flushData(f) })
		c.Exec(func() error { return cr.segments.set(s) })
	}
	cr.done <- struct{}{}
}

func (cr createOperation) Context() context.Context {
	return cr.ctx
}

func (cr createOperation) String() string {
	return fmt.Sprintf("[OP] Create - Channel %stream | File %stream", cr.channelKey, cr.FileKey())
}
