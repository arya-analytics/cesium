package kv

import (
	"bytes"
	"cesium/internal/errutil"
)

// ||||||| GENERATE |||||||

func CompositeKey(sep string, elems ...interface{}) []byte {
	b := new(bytes.Buffer)
	cw := errutil.NewCatchWrite(b)
	for i, e := range elems {
		if i != 0 {
			cw.Write([]byte(sep))
		}
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

func DashedCompositeKey(elems ...interface{}) []byte {
	return CompositeKey("-", elems...)
}
