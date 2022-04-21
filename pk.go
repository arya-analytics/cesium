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
