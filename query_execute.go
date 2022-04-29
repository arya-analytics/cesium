package cesium

import (
	"cesium/internal/kv"
	"cesium/util/wg"
	"context"
	log "github.com/sirupsen/logrus"
	"go/types"
	"io"
)

// |||||| RETRIEVE ||||||

func newRetrieveRunService(skv segmentKV, ckv channelKV) queryExecutor {
	return &retrieveQueryExecutor{parse: retrieveParse{skv: skv, ckv: ckv}}
}

type retrieveQueryExecutor struct {
	parse retrieveParse
	queue chan<- []retrieveOperation
}

func (rp *retrieveQueryExecutor) exec(ctx context.Context, q query) (err error) {
	wg, err := rp.parse.parse(ctx, q)
	if err != nil {
		return err
	}
	s := getStream[types.Nil, RetrieveResponse](q)
	go func() {
		wg.Wait()
		s.res <- RetrieveResponse{Err: io.EOF}
	}()
	rp.queue <- wg.Items
	return nil
}

// |||||| CREATE ||||||

type createQueryExecutor struct {
	fa    segmentAllocator
	skv   segmentKV
	ckv   channelKV
	queue chan<- []createOperation
}

func (cp *createQueryExecutor) exec(ctx context.Context, q query) error {
	cPKs, err := channelKeys(q, true)
	if err != nil {
		return err
	}
	if err := cp.ckv.lock(cPKs...); err != nil {
		return err
	}
	s := getStream[CreateRequest, CreateResponse](q)
	parse := createParse{
		skv:       cp.skv,
		allocator: cp.fa,
		stream:    s,
	}
	var w wg.Slice[createOperation]
	go func() {
	o:
		for {
			select {
			case req, ok := <-s.req:
				if !ok {
					break o
				}
				w, err = parse.parse(ctx, req)
				if err != nil {
					s.res <- CreateResponse{Err: err}
					break o
				}
				cp.queue <- w.Items
			case <-ctx.Done():
				break o
			}
		}
		w.Wait()
		if err := cp.ckv.unlock(cPKs...); err != nil {
			log.Errorf("[RUNNER] failed to unlock channel %v: %v", cPKs, err)
		}
		s.res <- CreateResponse{Err: io.EOF}
		close(s.res)
	}()
	return nil
}

// |||| CREATE CHANNEL |||||

type channelCounter = kv.PersistedCounter[ChannelKey]

type createChannelQueryExecutor struct {
	ckv     channelKV
	counter channelCounter
}

func (cr *createChannelQueryExecutor) exec(_ context.Context, q query) (err error) {
	dr, ok := dataRate(q)
	if !ok {
		return newSimpleError(ErrInvalidQuery, "no data rate provided to create query")
	}
	ds, ok := density(q)
	if !ok {
		return newSimpleError(ErrInvalidQuery, "no density provided to create query")
	}
	npk, err := cr.counter.Increment()
	if err != nil {
		return err
	}
	c := Channel{Key: npk, DataRate: dr, DataType: ds}
	err = cr.ckv.set(c.Key, c)
	setQueryRecord[Channel](q, c)
	return err
}

// |||||| RETRIEVE CHANNEL ||||||

type retrieveChannelQueryExecutor struct {
	ckv channelKV
}

func (rc *retrieveChannelQueryExecutor) exec(_ context.Context, q query) error {
	_, ok := q.variant.(RetrieveChannel)
	if !ok {
		return nil
	}
	cpk, err := getChannelKey(q)
	if err != nil {
		return err
	}
	c, err := rc.ckv.get(cpk)
	setQueryRecord[Channel](q, c)
	return err
}
