package pk

import (
	"github.com/google/uuid"
	"io"
)

type PK uuid.UUID

const Size = 16

func New() PK {
	return PK(uuid.New())
}

func NewFromBytes(b []byte) PK {
	uid, err := uuid.FromBytes(b)
	if err != nil {
		panic(err)
	}
	return PK(uid)
}

func Read(r io.Reader) (PK, error) {
	b := make([]byte, Size)
	_, err := io.ReadFull(r, b)
	if err != nil {
		return PK{}, err
	}
	return NewFromBytes(b), nil
}

func (k PK) Bytes() []byte {
	b, err := uuid.UUID(k).MarshalBinary()
	if err != nil {
		panic(err)
	}
	return b
}

func (k PK) String() string {
	return uuid.UUID(k).String()
}
