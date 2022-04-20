package cesium

import (
	"cesium/util/errutil"
	"context"
	"go/types"
	"io"
	"sync"
)

type operation interface {
	filePK() PK
	exec(f KeyFile)
	sendError(err error)
	context() context.Context
}

type operationWaitGroup struct {
	ops  []operation
	done chan struct{}
}

func (d operationWaitGroup) wait() {
	c := 0
	for range d.done {
		c++
		if c >= len(d.ops) {
			return
		}
	}
}

// |||||| RETRIEVE ||||||

// || PARSE ||

type retrieveParse struct {
	skv segmentKV
}

func (rp retrieveParse) parse(ctx context.Context, q query) (opSet operationWaitGroup, err error) {
	cpk, err := channelPK(q)
	if err != nil {
		return opSet, err
	}
	tr := timeRange(q)
	segments, err := rp.skv.filter(tr, cpk)
	if err != nil {
		return opSet, err
	}
	opSet.done = make(chan struct{}, len(segments))
	s := getStream[types.Nil, RetrieveResponse](q)
	for _, seg := range segments {
		opSet.ops = append(opSet.ops, retrieveOperation{seg: seg, done: opSet.done, ctx: ctx, stream: s})
	}
	return opSet, nil
}

// || OP ||

type retrieveOperation struct {
	seg    Segment
	stream stream[types.Nil, RetrieveResponse]
	done   chan struct{}
	ctx    context.Context
}

func (ro retrieveOperation) filePK() PK {
	return ro.seg.FilePK
}

func (ro retrieveOperation) channelPK() PK {
	return ro.seg.ChannelPK
}

func (ro retrieveOperation) context() context.Context {
	return ro.ctx
}

func (ro retrieveOperation) sendError(err error) {
	ro.stream.res <- RetrieveResponse{Err: err}
}

func (ro retrieveOperation) exec(f KeyFile) {
	c := errutil.NewCatchReadWriteSeek(f)
	c.Seek(ro.seg.Offset, io.SeekStart)
	b := make([]byte, ro.seg.Size)
	c.Read(b)
	ro.seg.Data = b
	if c.Error() == io.EOF {
		panic("retrieve encountered unexpected EOF. this is a bug.")
	}
	ro.stream.res <- RetrieveResponse{Segments: []Segment{ro.seg}, Err: c.Error()}
	ro.done <- struct{}{}
}

func (ro retrieveOperation) offset() int64 {
	return ro.seg.Offset
}

// |||||| CREATE ||||||

// || PARSE ||

// || OP ||

const maxFileOffset = 1e10

type createParse struct {
	fpk PK
	skv segmentKV
	fa  *fileAllocate
}

func (cp createParse) parse(ctx context.Context, q query, req CreateRequest) (opSet operationWaitGroup, err error) {
	cPK, err := channelPK(q)
	if err != nil {
		return opSet, err
	}
	fpk, err := cp.fa.allocate(cPK)
	if err != nil {
		return opSet, err
	}
	s := getStream[CreateRequest, CreateResponse](q)
	done := make(chan struct{}, 1)
	return operationWaitGroup{
		done: done,
		ops: []operation{
			createOperation{
				ctx:  ctx,
				fpk:  fpk,
				kv:   cp.skv,
				s:    s,
				seg:  req.Segments,
				done: done},
		},
	}, nil
}

type createOperation struct {
	fpk  PK
	cpk  PK
	seg  []Segment
	kv   segmentKV
	s    stream[CreateRequest, CreateResponse]
	ctx  context.Context
	done chan struct{}
}

func (cr createOperation) filePK() PK {
	return cr.fpk
}

func (cr createOperation) channelPK() PK {
	return cr.cpk
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
	cr.done <- struct{}{}
}

func (cr createOperation) context() context.Context {
	return cr.ctx
}

const maxOffset = -2

func (cr createOperation) offset() int64 {
	return maxOffset
}

// |||||| FILE ALLOCATE  ||||||

type fileAllocate struct {
	mu    sync.Mutex
	files map[PK]Size
	skv   segmentKV
}

func newFileAllocate(skv segmentKV) *fileAllocate {
	return &fileAllocate{files: make(map[PK]Size), skv: skv}
}

func (fa *fileAllocate) allocate(cPK PK) (PK, error) {
	latestSeg, err := fa.skv.latest(cPK)
	if (latestSeg.Offset > maxFileOffset) || IsErrorOfType(err, ErrNotFound) {
		return fa.allocateNew()
	}
	if err != nil {
		return PK{}, err
	}
	fa.setFile(latestSeg.FilePK, Size(latestSeg.Offset)+latestSeg.Size)
	return latestSeg.FilePK, nil
}

func (fa *fileAllocate) allocateNew() (PK, error) {
	fa.mu.Lock()
	for pk, size := range fa.files {
		if size < maxFileOffset {
			fa.mu.Unlock()
			return pk, nil
		}
	}
	fa.mu.Unlock()
	nPk := NewPK()
	fa.setFile(nPk, 0)
	return NewPK(), nil
}

func (fa *fileAllocate) setFile(f PK, s Size) {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	fa.files[f] = s
}

// |||||| BATCHED OPERATION ||||||

type batchOperation[T operation] []T

func (brc batchOperation) filePK() PK {
	panic("not implemented")
}

func (brc batchOperation) offset() int64 {
	panic("not implemented")
}

func (brc batchOperation) channelPK() PK {
	panic("not implemented")
}

func (brc batchOperation) exec(f KeyFile) {
	for _, op := range brc {
		op.exec(f)
	}
}

func (brc batchOperation) sendError(err error) {
	for _, brc := range brc {
		brc.sendError(err)
	}
}

func (brc batchOperation) context() context.Context {
	if len(brc) > 0 {
		return brc[0].context()
	}
	return context.Background()
}
