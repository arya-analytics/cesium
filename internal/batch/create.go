package batch

type Create[F, C comparable] struct{}

type CreateOperation[F, C comparable] interface {
	Operation[F]
	ChannelKey() C
}

func (b *Create[F, C]) Exec(ops []CreateOperation[F, C]) (oOps []Operation[F]) {
	for _, bo := range batchByFileKey[F, CreateOperation[F, C]](ops) {
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
