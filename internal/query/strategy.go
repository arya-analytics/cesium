package query

import (
	"context"
	"github.com/arya-analytics/cesium/internal/errutil"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/shut"
)

type Request = any

type Executor[Q Query] interface {
	Exec(ctx context.Context, query Q) error
}

type Writer[Q Query] interface {
	New(r Executor) Q
}

type Hook[Q Query] interface {
	Exec(q Q) error
}

type Validator[Q Query] = Hook[Q]

type IteratorProxy[
	F comparable,
	Q Query,
	O operation.Operation[F],
	R Request,
] interface {
	Open(ctx IteratorContext[F, Q, O, R]) error
}

type IteratorContext[
	F comparable,
	Q Query,
	O operation.Operation[F],
	R Request,
] struct {
	Ctx         context.Context
	ShutdownSig chan shut.Signal
	Query       Q
	Parser      Parser[F, Q, O, R]
}

type Parser[
	F comparable,
	Q Query,
	O operation.Operation[F],
	R Request,
] interface {
	Parse(q Q) []O
}

type Strategy[
	F comparable,
	Q Query,
	O operation.Operation[F],
	R Request,
] struct {
	Writer   Writer[Q]
	Shutdown shut.Shutdown
	Hooks    struct {
		PreAssembly   []Hook[Q]
		PostAssembly  []Hook[Q]
		PostExecution []Hook[Q]
	}
	Parser          Parser[F, Q, O, R]
	IteratorFactory IteratorProxy[F, Q, O, R]
}

func (s *Strategy[F, Q, O, R]) New() Q {
	return s.Writer.New(s)
}

func (s *Strategy[F, Q, O, R]) Exec(ctx context.Context, query Q) error {
	if err := s.execHooks(s.Hooks.PreAssembly, query); err != nil {
		return err
	}
	s.Shutdown.Go(func(sig chan shut.Signal) error {
		iterCtx := IteratorContext[F, Q, O, R]{
			Ctx:         ctx,
			ShutdownSig: sig,
			Query:       query,
			Parser:      s.Parser,
		}
		if err := s.IteratorFactory.Open(iterCtx); err != nil {
			return err
		}
		return s.execHooks(s.Hooks.PostExecution, query)
	})
	return s.execHooks(s.Hooks.PostAssembly, query)
}

func (s *Strategy[F, Q, O, R]) execHooks(hooks []Hook[Q], query Q) error {
	c := &Catch[Q]{Query: query, CatchSimple: *errutil.NewCatchSimple(errutil.WithAggregation())}
	for _, h := range hooks {
		c.Exec(h.Exec)
	}
	return c.Error()
}
