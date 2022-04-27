package cesium

import (
	"cesium/alamos"
	"cesium/util/errutil"
	"context"
	"fmt"
	log "github.com/sirupsen/logrus"
	"go/types"
	"io"
	"sync"
	"time"
)

type operation interface {
	filePK() int
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
	ckv channelKV
	skv segmentKV
}

func (rp retrieveParse) parse(ctx context.Context, q query) (opSet operationWaitGroup, err error) {
	cPKs, err := channelPKs(q, true)
	if err != nil {
		return opSet, err
	}
	if _, err := rp.ckv.getMultiple(cPKs...); err != nil {
		return opSet, err
	}
	tr := timeRange(q)
	var segments []Segment
	for _, cpk := range cPKs {
		nSegments, err := rp.skv.filter(tr, cpk)
		if err != nil {
			return opSet, err
		}
		if len(nSegments) == 0 {
			return opSet, newSimpleError(ErrNotFound, "no segments found to satisfy query")
		}
		segments = append(segments, nSegments...)
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
	return ro.seg.filePk
}

func (ro retrieveOperation) context() context.Context {
	return ro.ctx
}

func (ro retrieveOperation) sendError(err error) {
	ro.stream.res <- RetrieveResponse{Err: err}
}

func (ro retrieveOperation) exec(f keyFile) {
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
		"[OP] Retrieve - Channel %s | File %s | offset %d | size %d",
		ro.seg.ChannelPK,
		ro.filePK(),
		ro.offset(),
		ro.seg.size,
	)
}

// |||||| CREATE ||||||

// || PARSE ||

// || OP ||

const maxFileOffset = 1e9

type createParse struct {
	fpk    PK
	skv    segmentKV
	fa     *fileAllocate
	cPKs   []PK
	stream *stream[CreateRequest, CreateResponse]
}

func (cp createParse) parse(ctx context.Context, req CreateRequest) (opWg operationWaitGroup, err error) {
	opWg.done = make(chan struct{}, len(req.Segments))
	for _, seg := range req.Segments {
		fpk, err := cp.fa.allocate(seg.ChannelPK)
		if err != nil {
			return opWg, err
		}
		opWg.ops = append(opWg.ops, createOperation{
			ctx:  ctx,
			fpk:  fpk,
			cpk:  seg.ChannelPK,
			kv:   cp.skv,
			s:    cp.stream,
			seg:  req.Segments,
			done: opWg.done,
		})
	}
	return opWg, nil
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

func (cr createOperation) sendError(err error) {
	cr.s.res <- CreateResponse{Err: err}
}

func (cr createOperation) exec(f keyFile) {
	tot := time.Now()
	if _, err := f.Seek(0, io.SeekEnd); err != nil {
		cr.sendError(err)
		return
	}
	seek := time.Since(tot)
	var (
		flushTot time.Duration
		kvTot    time.Duration
		flushMax time.Duration
	)
	for _, s := range cr.seg {
		c := errutil.NewCatchReadWriteSeek(f)
		s.filePk = f.PKV()
		s.ChannelPK = cr.cpk
		s.size = s.Size()
		s.offset = c.Seek(0, io.SeekCurrent)
		flushStart := time.Now()
		c.Exec(func() error { return s.flushData(f) })
		flushDur := time.Since(flushStart)
		flushTot += flushDur
		kvStart := time.Now()
		if flushDur > flushMax {
			flushMax = flushDur
		}
		c.Exec(func() error { return cr.kv.set(s) })
		kvTot += time.Since(kvStart)
		if c.Error() != nil {
			if c.Error() == io.EOF {
				panic(io.ErrUnexpectedEOF)
			}
			cr.s.res <- CreateResponse{Err: c.Error()}
		}
	}
	t0 := time.Now()
	cr.done <- struct{}{}
	d := time.Since(t0)
	log.Infof("Total: %s | Seek %s | Flush %s | KV %s | Done %s | Flush Max %s",
		time.Since(tot),
		seek,
		flushTot,
		kvTot,
		d,
		flushMax,
	)
}

func (cr createOperation) context() context.Context {
	return cr.ctx
}

func (cr createOperation) String() string {
	return fmt.Sprintf("[OP] Create - Channel %s | File %s", cr.cpk, cr.filePK())
}

// |||||| FILE ALLOCATE  ||||||

type fileAllocateInfo struct {
	pk   PK
	size Size
}

type fileAllocate struct {
	mu        *sync.Mutex
	files     map[PK]fileAllocateInfo
	skv       segmentKV
	allocTime alamos.Duration
}

func newFileAllocate(skv segmentKV, exp alamos.Experiment) *fileAllocate {
	return &fileAllocate{
		files:     make(map[PK]fileAllocateInfo),
		mu:        &sync.Mutex{},
		skv:       skv,
		allocTime: alamos.NewSeriesDuration(exp, "cesium.fileAllocate.allocate"),
	}
}

func (fa *fileAllocate) allocate(cpk PK) (PK, error) {
	//fa.allocTime.Start()
	//defer fa.allocTime.Stop()
	fa.mu.Lock()
	defer fa.mu.Unlock()
	latestSeg, err := fa.skv.latest(cpk)
	if IsErrorOfType(err, ErrNotFound) {
		log.Debug("[cesium.fileAllocate]: Channel has no segments")
		fEntry, ok := fa.files[cpk]
		if ok {
			return fEntry.pk, nil
		}
		return fa.allocateNew(cpk)
	}
	if err != nil {
		panic(err)
	}

	fa.setFile(cpk, Size(latestSeg.offset)+latestSeg.size, latestSeg.filePk)
	if latestSeg.offset > maxFileOffset {
		log.Debugf("[FALLOC]: File %s filled up. Allocating new file.", latestSeg.filePk)
		return fa.allocateNew(cpk)
	}
	return latestSeg.filePk, nil
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
