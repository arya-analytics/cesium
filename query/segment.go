package query

import (
	"caesium/telem"
	"github.com/google/uuid"
)

type Segment struct {
	PK    uuid.UUID
	Start telem.TimeStamp
	Size  telem.Size
	Data  []byte
}
