package caesium

import (
	"context"
	"encoding/binary"
	"golang.org/x/sync/semaphore"
	"io"
	"sync"
)

type Operation interface {
	FileKey() PK
	Exec(ctx context.Context, f KeyFile) error
}

type Persist interface {
	Exec(ctx context.Context, ops ...Operation) []error
}

type persist struct {
	sem *semaphore.Weighted
	kfs *KFS
}

func newPersist(kfs *KFS) Persist {
	return persist{sem: semaphore.NewWeighted(50), kfs: kfs}
}

func (p persist) Exec(ctx context.Context, ops ...Operation) []error {
	errors := make([]error, len(ops))
	wg := sync.WaitGroup{}
	for i, op := range ops {
		wg.Add(1)
		if err := p.sem.Acquire(ctx, 1); err != nil {
			errors[i] = err
			break
		}
		go func(i int, op Operation) {
			defer wg.Done()
			defer p.sem.Release(1)
			f, err := p.kfs.Acquire(op.FileKey())
			if err != nil {
				errors[i] = err
				return
			}
			errors[i] = op.Exec(ctx, f)
			p.kfs.Release(op.FileKey())
		}(i, op)
	}
	wg.Wait()
	return errors
}

func Read[T any](r io.Reader, v T) error {
	return binary.Read(r, binary.LittleEndian, v)
}

func Write[T any](w io.Writer, v T) error {
	return binary.Write(w, binary.LittleEndian, v)
}
