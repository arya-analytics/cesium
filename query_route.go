package cesium

import "context"

type queryRouter struct {
	CreateChannel   execFunc
	RetrieveChannel execFunc
	Create          execFunc
	Retrieve        execFunc
	Delete          execFunc
}

func (r queryRouter) exec(ctx context.Context, q query) error {
	switch q.Variant().(type) {
	case CreateChannel:
		return r.execVariant(ctx, r.CreateChannel, q)
	case RetrieveChannel:
		return r.execVariant(ctx, r.RetrieveChannel, q)
	case Create:
		return r.execVariant(ctx, r.Create, q)
	case Retrieve:
		return r.execVariant(ctx, r.Retrieve, q)
	case Delete:
		return r.execVariant(ctx, r.Delete, q)
	}
	panic("invalid query variant received")
}

func (r queryRouter) execVariant(ctx context.Context, e execFunc, q query) error {
	if e == nil {
		panic("received unknown variant")
	}
	return e(ctx, q)
}
