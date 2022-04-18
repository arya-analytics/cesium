package errutil

import (
	"cesium/util/binary"
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
	if err := binary.Write(c.w, data); err != nil {
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
	if err := binary.Read(c.w, data); err != nil {
		c.e = err
	}
}

func (c *CatchRead) Error() error {
	return c.e
}

type CatchReadWriteSeek struct {
	w io.ReadWriteSeeker
	e error
}

func NewCatchReadWriteSeek(r io.ReadWriteSeeker) *CatchReadWriteSeek {
	return &CatchReadWriteSeek{w: r}
}

func (c *CatchReadWriteSeek) Read(data interface{}) {
	if c.e != nil {
		return
	}
	if err := binary.Read(c.w, data); err != nil {
		c.e = err
	}
}

func (c *CatchReadWriteSeek) Seek(offset int64, whence int) int64 {
	if c.e != nil {
		return 0
	}
	ret, err := c.w.Seek(offset, whence)
	if err != nil {
		c.e = err
	}
	return ret
}

func (c *CatchReadWriteSeek) Write(data interface{}) {
	if c.e != nil {
		return
	}
	if err := binary.Write(c.w, data); err != nil {
		c.e = err
	}
}

func (c *CatchReadWriteSeek) Exec(action func() error) {
	if c.e != nil {
		return
	}
	if err := action(); err != nil {
		c.e = err
	}
}

func (c *CatchReadWriteSeek) Error() error {
	return c.e
}
