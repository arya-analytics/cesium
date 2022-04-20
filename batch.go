package cesium

import (
	"sort"
)

// |||||| INTERFACE ||||||

type batch interface {
	exec([]operation) ([]operation, error)
}

// |||||| RETRIEVE ||||||

type retrieveBatch struct{}

func filterOperations[T operation](ops []operation) (conc []T) {
	for _, op := range ops {
		_, ok := op.(T)
		if ok {
			conc = append(conc, op.(T))
		}
	}
	return
}

func (r retrieveBatch) exec(ops []operation) ([]operation, error) {
	cOps := filterOperations[retrieveOperation](ops)
	fileBatched := batchByFilePK(cOps)
	var oOps []operation
	for _, batchedOps := range fileBatched {
		sortByOffset(batchedOps)
		oOps = append(oOps, batchOperation[retrieveOperation](batchedOps))
	}
	return oOps, nil
}

func batchByFilePK[T operation](ops []T) map[PK][]T {
	b := make(map[PK][]T)
	for _, op := range ops {
		b[op.filePK()] = append(b[op.filePK()], op)
	}
	return b
}

func sortByOffset(ops []retrieveOperation) {
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].offset() < ops[j].offset()
	})
}

// |||||| CREATE ||||||

type createBatch struct{}

func (c createBatch) exec(ops []operation) ([]operation, error) {
	cOps := filterOperations[createOperation](ops)
	fileBatched := batchByFilePK(cOps)
	var oOps []operation
	for _, batchedOps := range fileBatched {
		batchedByChannelPK := batchByChannelPK(batchedOps)
		op := make(batchOperation[createOperation], 0, len(batchedByChannelPK))
		for _, bOps := range batchedByChannelPK {
			op = append(op, bOps...)
		}
		oOps = append(oOps, op)
	}
	return oOps, nil
}

func batchByChannelPK(ops []createOperation) map[PK][]createOperation {
	bo := map[PK][]createOperation{}
	for _, op := range ops {
		bo[op.channelPK()] = append(bo[op.channelPK()], op)
	}
	return bo
}

// |||||| CHAIN BATCH ||||

type batchSet []batch

func (b batchSet) exec(ops []operation) ([]operation, error) {
	var oOps []operation
	for _, b := range b {
		bOps, _ := b.exec(ops)
		oOps = append(oOps, bOps...)
	}
	return oOps, nil
}
