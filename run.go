package cesium

import (
	"context"
	log "github.com/sirupsen/logrus"
	"go/types"
	"io"
)

type run struct {
	services   []runService
	batchQueue chan<- operation
}

type runService interface {
	exec(ctx context.Context, queue chan<- operation, q query) (handled bool, err error)
}

func (r *run) exec(ctx context.Context, q query) error {
	log.Infof("[cesium.Runner] executing query %s", q)
	for _, svc := range r.services {
		handled, err := svc.exec(ctx, r.batchQueue, q)
		if handled {
			return err
		}
	}
	panic("no service handled query. this is a bug.")
}

// |||||| RETRIEVE ||||||

func newRetrieveRunService(skv segmentKV, ckv channelKV) runService {
	return &retrieveRunService{parse: retrieveParse{skv: skv, ckv: ckv}}
}

type retrieveRunService struct {
	parse retrieveParse
}

func (rp *retrieveRunService) exec(ctx context.Context, queue chan<- operation, q query) (handled bool, err error) {
	_, ok := q.variant.(Retrieve)
	if !ok {
		return false, nil
	}
	opWg, err := rp.parse.parse(ctx, q)
	if err != nil {
		return true, err
	}
	s := getStream[types.Nil, RetrieveResponse](q)
	go func() {
		log.Info("[RUNNER] waiting for get to finish")
		opWg.wait()
		s.res <- RetrieveResponse{Err: io.EOF}
	}()
	go func() {
		for _, op := range opWg.ops {
			queue <- op
		}
	}()
	return true, nil
}

// |||||| CREATE ||||||

func newCreateRunService(fa *fileAllocate, skv segmentKV, ckv channelKV) runService {
	return &createRunService{fa: fa, skv: skv, ckv: ckv}
}

type createRunService struct {
	fa  *fileAllocate
	skv segmentKV
	ckv channelKV
}

func (cp *createRunService) exec(ctx context.Context, queue chan<- operation, q query) (handled bool, err error) {
	_, ok := q.variant.(Create)
	if !ok {
		return false, nil
	}
	cPKs, err := channelPKs(q, true)
	if err != nil {
		return true, err
	}
	log.Infof("[RUNNER] acquiring write lock on channels %v", cPKs)
	if err := cp.ckv.lock(cPKs...); err != nil {
		return true, err
	}
	log.Info("[cesium.createRunService] acquired write lock on channels")
	s := getStream[CreateRequest, CreateResponse](q)
	parse := createParse{
		skv:    cp.skv,
		fa:     cp.fa,
		cPKs:   cPKs,
		stream: s,
	}
	var opWg operationWaitGroup
	go func() {
	o:
		for {
			select {
			case req, ok := <-s.req:
				if !ok {
					break o
				}
				opWg, err = parse.parse(ctx, req)
				if err != nil {
					s.res <- CreateResponse{Err: err}
					break o
				}
				for _, op := range opWg.ops {
					queue <- op
				}
			case <-ctx.Done():
				break o
			}
		}
		log.Debug("[cesium.createRunService] waiting for create to finish")
		opWg.wait()
		log.Debug("[cesium.createRunService] releasing write lock on channels")
		if err := cp.ckv.unlock(cPKs...); err != nil {
			log.Errorf("[RUNNER] failed to unlock channel %v: %v", cPKs, err)
		}
		log.Debug("[cesium.createRunService] released write lock on channels. sending eof.")
		s.res <- CreateResponse{Err: io.EOF}
		log.Debug("[cesium.createRunService] sent eof. closing response pipe.")
		close(s.res)
	}()
	return true, nil
}

// |||| CREATE CHANNEL |||||

func newCreateChannelRunService(ckv channelKV) runService {
	return &createChannelRunService{ckv: ckv}
}

type createChannelRunService struct {
	ckv channelKV
}

func (cr *createChannelRunService) exec(_ context.Context, _ chan<- operation, q query) (handled bool, err error) {
	_, ok := q.variant.(CreateChannel)
	if !ok {
		return false, nil
	}
	handled = true
	dr, ok := dataRate(q)
	if !ok {
		return handled, newSimpleError(ErrInvalidQuery, "no data rate provided to create query")
	}
	ds, ok := density(q)
	if !ok {
		return handled, newSimpleError(ErrInvalidQuery, "no density provided to create query")
	}
	c := Channel{PK: NewPK(), DataRate: dr, DataType: ds}
	err = cr.ckv.set(c.PK, c)
	setQueryRecord[Channel](q, c)
	return handled, err
}

// |||||| RETRIEVE CHANNEL ||||||

func newRetrieveChannelRunService(ckv channelKV) runService {
	return &retrieveChannelRunService{ckv: ckv}
}

type retrieveChannelRunService struct {
	ckv channelKV
}

func (rc *retrieveChannelRunService) exec(_ context.Context, _ chan<- operation, q query) (handled bool, err error) {
	_, ok := q.variant.(RetrieveChannel)
	if !ok {
		return false, nil
	}
	handled = true
	cpk, err := channelPK(q)
	if err != nil {
		return handled, err
	}
	c, err := rc.ckv.get(cpk)
	setQueryRecord[Channel](q, c)
	return handled, err
}

// |||||| BATCH RUNNER ||||||

type batchRunner struct {
	persist Persist
	batch   batch
}

func (br batchRunner) exec(ops []operation) {
	bOps := br.batch.exec(ops)
	log.Infof("[cesium.Batch] executing %v operations on persist", len(bOps))
	br.persist.Exec(bOps)
}
