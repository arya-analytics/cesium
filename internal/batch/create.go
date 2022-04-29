package batch

type Create[F, C comparable, T CreateOperation[F, C]] struct{}

type CreateOperation[F, C comparable] interface {
	Operation[F]
	ChannelKey() C
}

func (b *Create[F, C, T]) Exec(ops []T) (oOps []Operation[F]) {
	bops := make(map[F]OperationSet[F, CreateOperation[F, C]])
	for _, op := range ops {
		bops[op.FileKey()] = append(bops[op.FileKey()], op)
	}
	for _, bo := range bops {
		for _, boC := range batchByChannelKey(bo) {
			oOps = append(oOps, boC)
		}
	}
	return oOps
}

func batchByChannelKey[F, C comparable](ops []CreateOperation[F, C]) map[C]OperationSet[F, CreateOperation[F, C]] {
	b := make(map[C]OperationSet[F, CreateOperation[F, C]])
	for _, op := range ops {
		b[op.ChannelKey()] = append(b[op.ChannelKey()], op)
	}
	return b
}
