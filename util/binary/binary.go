package binary

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
)

func Write(w io.Writer, data interface{}) (err error) {
	return binary.Write(w, Encoding(), data)
}

func Read(r io.Reader, data interface{}) (err error) {
	return binary.Read(r, Encoding(), data)
}

func Marshal(data interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	if err := Write(buf, data); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func Encoding() binary.ByteOrder {
	return binary.BigEndian
}

func ToFloat64(b []byte) []float64 {
	f64 := make([]float64, len(b)/8)
	for i := 0; i < len(b); i += 8 {
		f64[i/8] = math.Float64frombits(binary.BigEndian.Uint64(b[i:]))
	}
	return f64
}
