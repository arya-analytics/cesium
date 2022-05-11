package query

import (
	"github.com/arya-analytics/cesium/internal/operation"
)

type Iterator[REQ Request] interface {
	Next(req REQ) (last bool)
}

type IteratorFactory[F comparable, O operation.Operation[F], REQ Request] interface {
	New(ctx Context[F, O, REQ]) (Iterator[REQ], error)
}
