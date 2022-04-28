package batch

import (
	"cesium/kfs"
	"context"
	"sort"
)

type RetrieveBatch[K comparable] struct{}

type RetrieveOperation[K comparable] interface {
	Operation[K]
	Offset() int
}

func (r *RetrieveBatch[K]) Exec(ops []RetrieveOperation[K]) (oOps []Operation[K]) {
	for _, bo := range batchByFileKey[K, RetrieveOperation[K]](ops) {
		sortByOffset[K](bo)
		oOps = append(oOps, bo)
	}
	return oOps
}

type OperationSet[K comparable, T Operation[K]] []T

func (o OperationSet[K, T]) Context() context.Context {
	return o[0].Context()
}

func (o OperationSet[K, T]) FileKey() K {
	return o[0].FileKey()
}

func (o OperationSet[K, T]) Exec(f kfs.File) {
	for _, op := range o {
		op.Exec(f)
	}
}

func (o OperationSet[K, T]) SendError(err error) {
	for _, op := range o {
		op.SendError(err)
	}
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
