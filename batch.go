package cesium

import (
	"github.com/arya-analytics/cesium/internal/core"
	"github.com/arya-analytics/x/confluence"
	"sort"
)

// |||||| RETRIEVE ||||||

type retrieveBatch confluence.Transform[[]retrieveOperation]

func newRetrieveBatch() retrieveSegment {
	rb := &retrieveBatch{}
	rb.Transform = rb.batch
	return rb
}

func (rb *retrieveBatch) batch(ctx confluence.Context, ops []retrieveOperation) ([]retrieveOperation, bool) {
	if len(ops) == 0 {
		return nil, false
	}
	// group the operations by file key.
	files := make(map[core.FileKey]retrieveOperationSet)
	for _, op := range ops {

		files[op.FileKey()] = retrieveOperationSet{
			Set: append(files[op.FileKey()].Set, op),
		}
	}
	// order the operations by their offset in the file,
	oOps := make([]retrieveOperation, len(files))
	for i, opSet := range files {
		sort.Slice(opSet, func(i, j int) bool { return opSet.Set[i].Offset() < opSet.Set[j].Offset() })
		oOps[i] = opSet
	}
	return oOps, true
}

// |||||| CREATE ||||||

type createBatch confluence.Transform[[]createOperation]

func newCreateBatch() createSegment {
	cb := &createBatch{}
	cb.Transform = cb.batch
	return cb
}

func (cb *createBatch) batch(ctx confluence.Context, ops []createOperation) ([]createOperation, bool) {
	if len(ops) == 0 {
		return nil, false
	}
	// group the operations by file key.
	files := make(map[core.FileKey]createOperationSet)
	for _, op := range ops {
		files[op.FileKey()] = createOperationSet{Set: append(files[op.FileKey()].Set, op)}
	}
	// order the operations by their channel key.
	for _, ops := range files {
		sort.Slice(ops, func(i, j int) bool { return ops.Set[i].ChannelKey() > ops.Set[j].ChannelKey() })
	}
	oOps := make([]createOperation, len(ops))
	for i, opSet := range files {
		oOps[i] = opSet
	}
	return oOps, true
}
