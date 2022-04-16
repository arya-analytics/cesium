package binary

import (
	"encoding/binary"
	"io"
)

func Write(w io.Writer, data interface{}) (err error) {
	return binary.Write(w, binary.LittleEndian, data)
}

func Read(r io.Reader, data interface{}) (err error) {
	return binary.Read(r, binary.LittleEndian, data)
}
