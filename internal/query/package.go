package query

import (
	"context"
	"github.com/arya-analytics/cesium/internal/errutil"
	"github.com/arya-analytics/cesium/internal/operation"
	"github.com/arya-analytics/cesium/shut"
)

type Assembler[F comparable, Q Query, O operation.Operation[F]] interface {
	New(p *Package[F, Q, O]) Q
}

type Validator[Q Query] interface {
	Validate(q Q) error
}

type Parser[F comparable, Q Query, O operation.Operation[F]] interface {
	Parse(q Q) []O
}

type Executor[F comparable, Q Query, O operation.Operation[F]] interface {
	Exec(ctx ExecutionContext[F, Q, O]) error
}

type Hook[Q Query] interface {
	Exec(q Q) error
}

type Package[F comparable, Q Query, O operation.Operation[F]] struct {
	Assembler   Assembler[F, Q, O]
	Validators  []Validator[Q]
	Parser      Parser[F, Q, O]
	Executor    Executor[F, Q, O]
	Shutdown    shut.Shutdown
	BeforeHooks []Hook[Q]
	AfterHooks  []Hook[Q]
}

func (p *Package[F, Q, O]) New() Q {
	return p.Assembler.New(p)
}

type ExecutionContext[F comparable, Q Query, O operation.Operation[F]] struct {
	Ctx         context.Context
	ShutdownSig chan shut.Signal
	Query       Q
	Parser      Parser[F, Q, O]
}

func (p *Package[F, Q, O]) Exec(ctx context.Context, q Q) error {
	c := &Catch[Q]{q: q, CatchSimple: *errutil.NewCatchSimple(errutil.WithAggregation())}
	for _, v := range p.Validators {
		c.Exec(v.Validate)
	}
	for _, h := range p.BeforeHooks {
		c.Exec(h.Exec)
	}
	p.Shutdown.Go(func(sig chan shut.Signal) error {
		c.Exec(func(q Q) error {
			return p.Executor.Exec(ExecutionContext[F, Q, O]{ctx, sig, q, p.Parser})
		})
		for _, h := range p.BeforeHooks {
			c.Exec(h.Exec)
		}
		return c.Error()
	})
	return c.Error()
}

type Catch[Q Query] struct {
	errutil.CatchSimple
	q Q
}

func (c *Catch[Q]) Exec(f func(q Q) error) {
	c.CatchSimple.Exec(func() error { return f(c.q) })
}
