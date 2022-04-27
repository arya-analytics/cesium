package persist

import (
	"cesium/kfs"
	"context"
	"golang.org/x/sync/semaphore"
)

type Persist struct {
	sem *semaphore.Weighted
	kfs kfs.FS
}

type Operation interface {
	Context() context.Context
	FileKey() int
	SendError(error)
	Exec(f kfs.File)
}

func NewPersist(kfs kfs.FS, maxProcs int64) *Persist {
	return &Persist{sem: semaphore.NewWeighted(maxProcs), kfs: kfs}
}

func (p *Persist) Exec(ops []Operation) {
	for _, op := range ops {
		_ = p.sem.Acquire(op.Context(), 1)
		go func(op Operation) {
			defer p.sem.Release(1)
			f, err := p.kfs.Acquire(op.FileKey())
			if err != nil {
				op.SendError(err)
				return
			}
			defer p.kfs.Release(op.FileKey())
			op.Exec(f)
		}(op)
	}
}
