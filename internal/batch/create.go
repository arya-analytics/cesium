package batch

type Create[K, C comparable] struct{}

type CreateOperation[K, C comparable] interface {
	Operation[K]
	ChannelKey() C
}

func (b *Create[K, C]) Exec(ops []CreateOperation[K, C]) (oOps []Operation[K]) {
	for _, bo := range batchByFileKey[K, CreateOperation[K, C]](ops) {
		for _, boC := range batchByChannelKey(bo) {
			oOps = append(oOps, boC)
		}
	}
	return oOps
}

func batchByChannelKey[K, C comparable](ops []CreateOperation[K, C]) map[C]OperationSet[K, CreateOperation[K, C]] {
	b := make(map[C]OperationSet[K, CreateOperation[K, C]])
	for _, op := range ops {
		b[op.ChannelKey()] = append(b[op.ChannelKey()], op)
	}
	return b
}
