package errutil

import (
	"encoding/binary"
	"io"
)

type CatchWrite struct {
	w io.Writer
	e error
}

func NewCatchWrite(w io.Writer) *CatchWrite {
	return &CatchWrite{w: w}
}

func (c *CatchWrite) Write(data interface{}) {
	if c.e != nil {
		return
	}
	if err := binary.Write(c.w, binary.LittleEndian, data); err != nil {
		c.e = err
	}
}

func (c *CatchWrite) Error() error {
	return c.e
}

type CatchRead struct {
	w io.Reader
	e error
}

func NewCatchRead(r io.Reader) *CatchRead {
	return &CatchRead{w: r}
}

func (c *CatchRead) Read(data interface{}) {
	if c.e != nil {
		return
	}
	if err := binary.Read(c.w, binary.LittleEndian, data); err != nil {
		c.e = err
	}
}

func (c *CatchRead) Error() error {
	return c.e
}
