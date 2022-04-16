package errutil

import (
	"caesium/persist"
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
	if err := persist.Write(c.w, data); err != nil {
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
	if err := persist.Read(c.w, data); err != nil {
		c.e = err
	}
}

func (c *CatchRead) Error() error {
	return c.e
}
