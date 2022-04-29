package batch

import (
	"sort"
)

type Retrieve[K comparable] struct{}

type RetrieveOperation[K comparable] interface {
	Operation[K]
	Offset() int
}

func (r *Retrieve[K]) Exec(ops []RetrieveOperation[K]) (oOps []Operation[K]) {
	for _, bo := range batchByFileKey[K, RetrieveOperation[K]](ops) {
		sortByOffset[K](bo)
		oOps = append(oOps, bo)
	}
	return oOps
}

func batchByFileKey[K comparable, T Operation[K]](ops []T) map[K]OperationSet[K, T] {
	b := make(map[K]OperationSet[K, T])
	for _, op := range ops {
		b[op.FileKey()] = append(b[op.FileKey()], op)
	}
	return b
}

func sortByOffset[K comparable](ops OperationSet[K, RetrieveOperation[K]]) {
	sort.Slice(ops, func(i, j int) bool { return ops[i].Offset() < ops[j].Offset() })
}
