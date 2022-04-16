package caesium

type Segment struct {
	PK    PK
	Start TimeStamp
	Size  Size
	Data  []byte
}
