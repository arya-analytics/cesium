package channel

import (
	"caesium/pk"
	"caesium/telem"
	"caesium/util/errutil"
	"io"
)

type Channel struct {
	PK       pk.PK
	DataRate telem.DataRate
	Density  telem.Density
}

func (c Channel) Flush(w io.Writer) error {
	cw := errutil.NewCatchWrite(w)
	cw.Write(c.PK)
	cw.Write(c.DataRate)
	cw.Write(c.Density)
	return cw.Error()
}

func (c Channel) Fill(r io.Reader) (Channel, error) {
	cr := errutil.NewCatchRead(r)
	cr.Read(&c.PK)
	cr.Read(&c.DataRate)
	cr.Read(&c.Density)
	return c, cr.Error()
}
