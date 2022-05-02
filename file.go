package cesium

import (
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/kfs"
)

type (
	fileKey    int16
	fileSystem = kfs.FS[fileKey]
	file       = kfs.File[fileKey]
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
