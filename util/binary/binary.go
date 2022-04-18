package binary

import (
	"encoding/binary"
	"io"
)

func Write(w io.Writer, data interface{}) (err error) {
	return binary.Write(w, Encoding(), data)
}

func Read(r io.Reader, data interface{}) (err error) {
	return binary.Read(r, Encoding(), data)
}

func Encoding() binary.ByteOrder {
	return binary.BigEndian
}
