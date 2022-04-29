package kv

import (
	"bytes"
	"cesium/internal/errutil"
)

// ||||||| GENERATE |||||||

func CompositeKey(elems ...interface{}) []byte {
	b := new(bytes.Buffer)
	cw := errutil.NewCatchWrite(b)
	for _, e := range elems {
		switch e.(type) {
		case string:
			cw.Write([]byte(e.(string)))
		default:
			cw.Write(e)
		}
	}
	if cw.Error() != nil {
		panic(cw.Error())
	}
	return b.Bytes()
}
