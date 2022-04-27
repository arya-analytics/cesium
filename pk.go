package cesium

import (
	"github.com/google/uuid"
)

type PK uuid.UUID

func NewPK() PK {
	return PK(uuid.New())
}

func (k PK) String() string {
	return uuid.UUID(k).String()
}

type KeyedObject interface {
	PKV() PK
}

func PKs[T KeyedObject](objects []T) []PK {
	pks := make([]PK, len(objects))
	for i, o := range objects {
		pks[i] = o.PKV()
	}
	return pks
}
