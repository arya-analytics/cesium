package query

import (
	"github.com/arya-analytics/cesium/internal/errutil"
)

type Catch[Q Query] struct {
	errutil.CatchSimple
	Query Q
}

func (c *Catch[Q]) Exec(f func(q Q) error) {
	c.CatchSimple.Exec(func() error { return f(c.Query) })
}
