package cesium

import (
	"sort"
)

// |||||| INTERFACE ||||||

type batch interface {
	exec([]operation) []operation
}

// |||||| RETRIEVE ||||||

type retrieveBatch struct{}

func filterOperations[T operation](ops []operation) (conc []T) {
	for _, op := range ops {
		v, ok := op.(T)
		if ok {
			conc = append(conc, v)
		}
	}
	return
}

func (r retrieveBatch) exec(ops []operation) (oOps []operation) {
	cOps := filterOperations[retrieveOperation](ops)
	for _, batchedOp := range batchByFilePK(cOps) {
		sortByOffset(batchedOp)
		oOps = append(oOps, batchOperation[retrieveOperation](batchedOp))
	}
	return oOps
}

func batchByFilePK[T operation](ops []T) map[PK][]T {
	b := make(map[PK][]T)
	for _, op := range ops {
		b[op.filePK()] = append(b[op.filePK()], op)
	}
	return b
}

func sortByOffset(ops []retrieveOperation) {
	sort.Slice(ops, func(i, j int) bool { return ops[i].offset() < ops[j].offset() })
}

// |||||| CREATE ||||||

type createBatchFileChannel struct{}

func (c createBatchFileChannel) exec(ops []operation) (oOps []operation) {
	cOps := filterOperations[createOperation](ops)
	fb := batchByFilePK(cOps)
	for _, batchedOps := range fb {
		bCPK := batchByChannelPK(batchedOps)
		op := make(batchOperation[createOperation], 0, len(bCPK))
		for _, bOps := range bCPK {
			op = append(op, bOps...)
		}
		oOps = append(oOps, op)
	}
	return oOps
}

func batchByChannelPK(ops []createOperation) map[PK][]createOperation {
	bo := make(map[PK][]createOperation)
	for _, op := range ops {
		bo[op.cpk] = append(bo[op.cpk], op)
	}
	return bo
}

// |||||| CHAIN BATCH ||||

type batchSet []batch

func (bs batchSet) exec(ops []operation) (oOps []operation) {
	for _, b := range bs {
		oOps = append(oOps, b.exec(ops)...)
	}
	return oOps
}
