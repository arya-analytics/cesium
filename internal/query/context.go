package query

import (
	"context"
	"github.com/arya-analytics/cesium/internal/operation"
	"sync"
)

const contextOptKey = "context"

func SetContext(q Query, ctx context.Context) {
	q.Set(contextOptKey, ctx)
}

func GetContext(q Query) context.Context {
	return q.GetRequired(contextOptKey).(context.Context)
}

type Context[
	F comparable,
	O operation.Operation[F],
	REQ Request,
] struct {
	Query     Query
	Parser    Parser[F, O, REQ]
	WaitGroup *sync.WaitGroup
}
