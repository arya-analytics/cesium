package query

import (
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/x/shutdown"
	"github.com/arya-analytics/x/alamos"
	"github.com/arya-analytics/x/util/errutil"
	"sync"


type Executor interface {
	Exec(query Query) error
}

type Factory[Q Query] interface {
	New() Q
}

type Hook interface {
	Exec(q Query) error
}

type HookSet []Hook

func (hooks HookSet) Exec(query Query) error {
	c := &Catch{Query: query, CatchSimple: *errutil.NewCatchSimple(errutil.WithAggregation())}
	for _, h := range hooks {
		c.Exec(h.Exec)
	}
	return c.Error()
}

type Parser[
F comparable,
O operation.Operation[F],
R Request,
] interface {
	Parse(q Query, r R) ([]O, error)
}

type Strategy[
F comparable,
O operation.Operation[F],
REQ Request,
RES Response,
] struct {
	Shutdown shutdown.Shutdown
	Hooks    struct {
		PreAssembly   HookSet
		PostAssembly  HookSet
		PostExecution HookSet
	}
	Parser      Parser[F, O, REQ]
	IterFactory IteratorFactory[F, O, REQ]
	Metrics     struct {
		Request alamos.Duration
	}
}

func (s *Strategy[F, O, REQ, RES]) Exec(query Query) error {
	if err := s.Hooks.PreAssembly.Exec(query); err != nil {
		return nil
	}

	stream := GetStream[REQ, RES](query)

	wg := &sync.WaitGroup{}
	setWaitGroup(query, wg)

	iterCtx := Context[F, O, REQ]{Query: query, Parser: s.Parser, WaitGroup: wg}
	iter, err := s.IterFactory.New(iterCtx)
	if err != nil {
		return err
	}

	s.Shutdown.Go(func(sig chan shutdown.Signal) error {
		sw := s.Metrics.Request.Stopwatch()
		sw.Start()
		defer sw.Stop()
	o:
		for {
			select {
			case <-sig:
				break o
			case <-query.Context().Done():
				return nil
			case request, ok := <-stream.Requests:
				if !ok {
					break o
				}
				if last := iter.Next(request); last {
					break o
				}
			}
		}
		wg.Wait()
		return s.Hooks.PostExecution.Exec(query)
	})

	return s.Hooks.PostAssembly.Exec(query)
}
