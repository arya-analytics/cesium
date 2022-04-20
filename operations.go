package cesium

import (
	"cesium/util/errutil"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go/types"
	"io"
	"sync"
)

type operation interface {
	filePK() PK
	exec(f keyFile)
	sendError(err error)
	context() context.Context
	String() string
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
	if len(segments) == 0 {
		return opSet, newSimpleError(ErrNotFound, "no segments found to satisfy query")
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
	stream *stream[types.Nil, RetrieveResponse]
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

func (ro retrieveOperation) exec(f keyFile) {
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

func (ro retrieveOperation) String() string {
	return fmt.Sprintf(
		"[OP] Retrieve - Channel %s | File %s | Offset %d | Size %d",
		ro.channelPK(),
		ro.filePK(),
		ro.offset(),
		ro.seg.Size,
	)
}

// |||||| CREATE ||||||

// || PARSE ||

// || OP ||

const maxFileOffset = 1e8

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
				cpk:  cPK,
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
	s    *stream[CreateRequest, CreateResponse]
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

func (cr createOperation) exec(f keyFile) {
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

func (cr createOperation) String() string {
	return fmt.Sprintf(
		"[OP] Create - Channel %s | File %s",
		cr.channelPK(),
		cr.filePK(),
	)
}

// |||||| FILE ALLOCATE  ||||||

type fileAllocateInfo struct {
	pk   PK
	size Size
}

type fileAllocate struct {
	mu    *sync.Mutex
	files map[PK]fileAllocateInfo
	skv   segmentKV
}

func newFileAllocate(skv segmentKV) *fileAllocate {
	return &fileAllocate{files: make(map[PK]fileAllocateInfo), mu: &sync.Mutex{}, skv: skv}
}

func (fa *fileAllocate) allocate(cpk PK) (PK, error) {
	fa.mu.Lock()
	defer fa.mu.Unlock()
	latestSeg, err := fa.skv.latest(cpk)
	fpk, _ := fa.files[cpk]
	log.Debugf(`[FALLOC]:
		Channel: %s
		Latest Segment File PK: %s
		Latest Mem File PK: %s
		`, cpk, latestSeg.FilePK, fpk.pk)
	if IsErrorOfType(err, ErrNotFound) {
		log.Error("[FALLOC]: Channel has no segments")
		fEntry, ok := fa.files[cpk]
		if ok {
			return fEntry.pk, nil
		}
		return fa.allocateNew(cpk)
	}
	if err != nil {
		panic(err)
	}

	fa.setFile(cpk, Size(latestSeg.Offset)+latestSeg.Size, latestSeg.FilePK)
	if latestSeg.Offset > maxFileOffset {
		log.Debugf("[FALLOC]: File %s filled up. Allocating new file.", latestSeg.FilePK)
		return fa.allocateNew(cpk)
	}
	return latestSeg.FilePK, nil
}

func (fa *fileAllocate) allocateNew(cpk PK) (PK, error) {
	for _, info := range fa.files {
		if info.size < maxFileOffset {
			return info.pk, nil
		}
	}
	fpk := NewPK()
	log.Infof("[FALLOC]: creating new file %s", fpk)
	fa.setFile(cpk, 0, fpk)
	log.Infof("[FALLOC]: total of %v unique files allocated", len(fa.files))
	return fpk, nil
}

func (fa *fileAllocate) setFile(cpk PK, s Size, fpk PK) {
	fa.files[cpk] = fileAllocateInfo{pk: fpk, size: s}
}

// |||||| BATCHED OPERATION ||||||

type batchOperation[T operation] []T

func (brc batchOperation[T]) filePK() PK {
	return brc[0].filePK()
}

func (brc batchOperation[T]) exec(f keyFile) {
	for _, op := range brc {
		op.exec(f)
	}
}

func (brc batchOperation[T]) sendError(err error) {
	for _, brc := range brc {
		brc.sendError(err)
	}
}

func (brc batchOperation[T]) context() context.Context {
	if len(brc) > 0 {
		return brc[0].context()
	}
	return context.Background()
}

func (brc batchOperation[T]) String() string {
	return fmt.Sprintf("[OP] Batch | Type - %T | Count - %v | File - %s", brc[0], len(brc), brc.filePK())
}
