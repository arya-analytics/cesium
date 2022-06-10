package operation

import (
	"context"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/kfs"
	"sync"
)

type Operation[F comparable] interface {
	// Context returns a context, that when canceled represents a forced abort of the operation.
	Context() context.Context
	// FileKey returns the key of the file to which the operation applies.
	FileKey() F
	// WriteError sends an error to the operation. This is only used for IO errors.
	WriteError(error)
	// BindWaitGroup adds the operation to the wait group. sync.WaitGroup.Done() should
	// be called in a defer statement by the implementer of Exec.
	BindWaitGroup(wg *sync.WaitGroup)
	// Exec is called by Persist to execute the operation. The provided file will have the key returned by FileKey.
	// The operation has a lock on the file during this time, and is free to make any modifications.
	Exec(f kfs.File[F])
}

type Set[F comparable, O Operation[F]] []O

func (s Set[F, T]) Context() context.Context {
	return s[0].Context()
}

func (s Set[F, T]) FileKey() F {
	return s[0].FileKey()
}

func (s Set[F, T]) Exec(f kfs.File[F]) {
	for _, op := range s {
		op.Exec(f)
	}
}

func (s Set[F, T]) WriteError(err error) {
	for _, op := range s {
		op.WriteError(err)
	}
}

func (s Set[F, T]) BindWaitGroup(wg *sync.WaitGroup) {
	for _, op := range s {
		op.BindWaitGroup(wg)
	}
}

// Parser is an entity that parses segment ranges into operations executable on disk.
type Parser[F comparable, O Operation[F]] interface {
	Parse(p *segment.Range) ([]O, error)
}
