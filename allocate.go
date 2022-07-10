package cesium

import (
	"github.com/arya-analytics/cesium/internal/allocate"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/core"
	"github.com/arya-analytics/x/confluence"
	"github.com/arya-analytics/x/signal"
)

type allocator struct {
	confluence.LinearTransform[[]createOperation, []createOperation]
	allocate.Allocator[channel.Key, core.FileKey, createOperation]
}

func newAllocator(counter *fileCounter, cfg allocate.Config) createSegment {
	a := &allocator{
		Allocator: allocate.New[channel.Key, core.FileKey, createOperation](counter, cfg),
	}
	a.ApplyTransform = a.allocate
	return a
}

func (a *allocator) allocate(
	ctx signal.Context,
	ops []createOperation,
) ([]createOperation, bool, error) {
	for i, fk := range a.Allocate(ops...) {
		ops[i].SetFileKey(fk)
	}
	return ops, true, nil
}
