package cesium

import (
	"golang.org/x/sync/semaphore"
	"sync"
)

type Persist interface {
	Exec(ops ...operation)
}

type persist struct {
	sem *semaphore.Weighted
	kfs *KFS
}

func newPersist(kfs *KFS) Persist {
	return &persist{sem: semaphore.NewWeighted(50), kfs: kfs}
}

func (p *persist) Exec(ops ...operation) {
	wg := sync.WaitGroup{}
	for i, op := range ops {
		wg.Add(1)
		if err := p.sem.Acquire(op.context(), 1); err != nil {
			op.sendError(err)
			break
		}
		go func(i int, op operation) {
			defer wg.Done()
			defer p.sem.Release(1)
			f, err := p.kfs.Acquire(op.filePK())
			if err != nil {
				op.sendError(err)
				return
			}
			op.exec(f)
			p.kfs.Release(op.filePK())
		}(i, op)
	}
	wg.Wait()
}
