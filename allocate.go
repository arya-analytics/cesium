package cesium

import (
	"github.com/arya-analytics/cesium/internal/allocate"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/core"
	"github.com/arya-analytics/x/confluence"
)

type allocator struct {
	confluence.Transform[[]createOperation]
	allocate.Allocator[channel.Key, core.FileKey, createOperation]
}

func newAllocator(counter *fileCounter, cfg allocate.Config) createSegment {
	a := &allocator{
		Allocator: allocate.New[channel.Key, core.FileKey, createOperation](counter, cfg),
	}
	a.Transform.Transform = a.allocate
	return a
}

func (a *allocator) allocate(
	ctx confluence.Context,
	ops []createOperation,
) ([]createOperation, bool, error) {
	for i, fk := range a.Allocate(ops...) {
		ops[i].SetFileKey(fk)
	}
	return ops, true, nil
}
