package cesium

import (
	"cesium/internal/allocate"
	"cesium/internal/errutil"
	"cesium/internal/lock"
	"cesium/internal/wg"
	"cesium/shut"
	"context"
	"io"
)

type (
	createStream     = stream[CreateRequest, CreateResponse]
	createWaitGroup  = wg.Slice[createOperation]
	segmentAllocator = allocate.Allocator[ChannelKey, fileKey, Segment]
)

// |||||| OPERATION ||||||

type createOperation struct {
	fileKey    fileKey
	channelKey ChannelKey
	segments   []Segment
	segmentKV  segmentKV
	stream     *createStream
	ctx        context.Context
	done       chan struct{}
}

// Context implements persist.Operation.
func (cr createOperation) Context() context.Context {
	return cr.ctx
}

// FileKey implements persist.Operation.
func (cr createOperation) FileKey() fileKey {
	return cr.fileKey
}

// SendError implements persist.Operation.
func (cr createOperation) SendError(err error) {
	cr.stream.res <- CreateResponse{Err: err}
}

// Exec implements persist.Operation.
func (cr createOperation) Exec(f file) {
	c := errutil.NewCatchReadWriteSeek(f, errutil.WithHooks(cr.SendError))
	c.Seek(0, io.SeekEnd)
	for _, s := range cr.segments {
		s.fileKey = f.Key()
		s.ChannelKey = cr.channelKey
		s.size = Size(s.Size())
		s.offset = c.Seek(0, io.SeekCurrent)
		c.Exec(func() error { return s.flushData(f) })
		c.Exec(func() error { return cr.segmentKV.set(s) })
	}
	cr.done <- struct{}{}
}

// ChannelKey implements batch.CreateOperation.
func (cr createOperation) ChannelKey() ChannelKey {
	return cr.channelKey
}

// |||||| PARSER ||||||

type createParser struct {
	skv       segmentKV
	allocator segmentAllocator
	stream    *createStream
}

func (cp createParser) parse(ctx context.Context, req CreateRequest) (cwg createWaitGroup) {
	cwg.Done = make(chan struct{}, len(req.Segments))
	fileKeys := cp.allocator.Allocate(req.Segments...)
	for i, seg := range req.Segments {
		cwg.Items = append(cwg.Items, createOperation{
			fileKey:    fileKeys[i],
			channelKey: seg.ChannelKey,
			segments:   req.Segments,
			segmentKV:  cp.skv,
			stream:     cp.stream,
			ctx:        ctx,
			done:       cwg.Done,
		})
	}
	return cwg
}

// |||||| EXECUTOR ||||||

type createQueryExecutor struct {
	allocator segmentAllocator
	skv       segmentKV
	ckv       channelKV
	queue     chan<- []createOperation
	lock      lock.Map[ChannelKey]
	shutdown  shut.Shutdown
}

// exec implements queryExecutor.
func (cp *createQueryExecutor) exec(ctx context.Context, q query) error {
	keys, err := channelKeys(q, true)
	if err != nil {
		return err
	}
	if err := cp.lock.Acquire(keys...); err != nil {
		return err
	}
	s := getStream[CreateRequest, CreateResponse](q)
	parse := createParser{skv: cp.skv, allocator: cp.allocator, stream: s}
	var w createWaitGroup
	cp.shutdown.Go(func(sig chan shut.Signal) error {
	o:
		for {
			select {
			case <-sig:
				break o
			case <-ctx.Done():
				return nil
			case req, ok := <-s.req:
				if !ok {
					break o
				}
				w = parse.parse(ctx, req)
				cp.queue <- w.Items
			}
		}
		w.Wait()
		cp.lock.Release(keys...)
		close(s.res)
		return nil
	})
	return nil
}
