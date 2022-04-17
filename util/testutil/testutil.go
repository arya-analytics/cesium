package testutil

import (
	"bytes"
	"cesium"
	"encoding/binary"
)

func RandFloat64Slice(n int) []float64 {
	s := make([]float64, n)
	for i := range s {
		s[i] = float64(i)
	}
	return s
}

func WriteFloat64Slice(s []float64) (*bytes.Buffer, error) {
	buf := bytes.NewBuffer(make([]byte, 0, len(s)*8))
	buf.Reset()
	err := binary.Write(buf, binary.LittleEndian, s)
	return buf, err
}

func RandomFloat64Bytes(n int) []byte {
	s := RandFloat64Slice(n)
	buf, err := WriteFloat64Slice(s)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func RandomFloat64Segment(n int) cesium.SegmentData {
	return RandomFloat64Bytes(n)
}
