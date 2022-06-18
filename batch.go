package cesium

import (
	"github.com/arya-analytics/cesium/internal/core"
	"github.com/arya-analytics/x/confluence"
	"sort"
)

// |||||| RETRIEVE ||||||

type retrieveBatch struct {
	confluence.Transform[[]retrieveOperation]
}

func newRetrieveBatch() retrieveSegment {
	rb := &retrieveBatch{}
	rb.Transform.Transform = rb.batch
	return rb
}

func (rb *retrieveBatch) batch(ctx confluence.Context, ops []retrieveOperation) ([]retrieveOperation, bool) {
	if len(ops) == 0 {
		return []retrieveOperation{}, false
	}
	// group the operations by file key.
	files := make(map[core.FileKey]retrieveOperationSet)
	for _, op := range ops {

		files[op.FileKey()] = retrieveOperationSet{
			Set: append(files[op.FileKey()].Set, op),
		}
	}
	// order the operations by their offset in the file,
	oOps := make([]retrieveOperation, 0, len(files))
	for _, opSet := range files {
		sort.Slice(opSet.Set, func(i, j int) bool {
			return opSet.Set[i].Offset() < opSet.Set[j].Offset()
		})
		oOps = append(oOps, opSet)
	}
	return oOps, true
}

// |||||| CREATE ||||||

type createBatch struct {
	confluence.Transform[[]createOperation]
}

func newCreateBatch() createSegment {
	cb := &createBatch{}
	cb.Transform.Transform = cb.batch
	return cb
}

func (cb *createBatch) batch(ctx confluence.Context, ops []createOperation) ([]createOperation, bool) {
	if len(ops) == 0 {
		return []createOperation{}, false
	}
	// group the operations by file key.
	files := make(map[core.FileKey]createOperationSet)
	for _, op := range ops {
		files[op.FileKey()] = createOperationSet{Set: append(files[op.FileKey()].Set, op)}
	}
	// order the operations by their channel key.
	oOps := make([]createOperation, 0, len(files))
	for _, fileOps := range files {
		sort.Slice(fileOps.Set, func(j, k int) bool {
			return fileOps.Set[j].
				ChannelKey() > fileOps.Set[k].ChannelKey()
		})
		oOps = append(oOps, fileOps)
	}
	return oOps, true
	return ops, true
}
