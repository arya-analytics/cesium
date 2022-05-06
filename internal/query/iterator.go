package query

import (
	"github.com/arya-analytics/cesium/internal/operation"
)

type Iterator[REQ Request] interface {
	Next(req REQ) bool
}

type IteratorProxy[F comparable, O operation.Operation[F], REQ Request] interface {
	Open(ctx Context[F, O, REQ]) (Iterator[REQ], error)
}
