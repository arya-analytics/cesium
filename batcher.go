package cesium

import (
	"context"
	"sort"
)

type batcher struct {
	pst Persist
}

func newBatcher(pst Persist) *batcher {
	return &batcher{pst: pst}
}

type batchedRetrieveOperation []retrieveOperation

func (brc batchedRetrieveOperation) filePK() PK {
	return brc[0].filePK()
}

func (brc batchedRetrieveOperation) exec(f KeyFile) {
	for _, op := range brc {
		op.exec(f)
	}
}

func (brc batchedRetrieveOperation) sendError(err error) {
	if len(brc) > 0 {
		brc[0].sendError(err)
	}
}

func (brc batchedRetrieveOperation) context() context.Context {
	if len(brc) > 0 {
		return brc[0].context()
	}
	return context.Background()
}

func (b *batcher) exec(ops ...operation) {
	batchedOps := make(map[PK]batchedRetrieveOperation)
	for _, op := range ops {
		rop := op.(retrieveOperation)
		batchedOps[rop.filePK()] = append(batchedOps[rop.filePK()], rop)
	}
	var nops []operation
	for _, nop := range batchedOps {
		sortRetrieveOps(nop...)
		nops = append(nops, nop)
	}
	b.pst.Exec(nops...)
}

// sorts the operations in asc order by offset
func sortRetrieveOps(ops ...retrieveOperation) {
	sort.Slice(ops, func(i, j int) bool {
		return ops[i].seg.Offset < ops[j].seg.Offset
	})
}
