package cesium

import (
	"cesium/internal/kv"
	"cesium/kfs"
)

type (
	fileKey int16
	file    = kfs.File[fileKey]
)

type fileCounter struct {
	kv.PersistedCounter
}

// Next implements allocate.NextDescriptor.
func (f *fileCounter) Next() fileKey {
	v, err := f.Increment()
	if err != nil {
		panic(err)
	}
	return fileKey(v)
}
