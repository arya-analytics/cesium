package query

import (
	"errors"
	"github.com/arya-analytics/x/errutil"
)

// |||||| ERRORS ||||||

var (
	ErrNotFound = errors.New("not found")
)

// |||||| CATCH ||||||

type Catch struct {
	errutil.CatchSimple
	Query Query
}

func (c *Catch) Exec(f func(query Query) error) {
	c.CatchSimple.Exec(func() error { return f(c.Query) })
}
