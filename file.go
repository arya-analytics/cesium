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

func newFileCounter(kve kv.KV, key []byte) (*fileCounter, error) {
	counter, err := kv.NewPersistedCounter(kve, []byte(fileCounterKey))
	return &fileCounter{PersistedCounter: *counter}, err
}

// Next implements allocate.NextDescriptor.
func (f *fileCounter) Next() fileKey {
	v, err := f.Increment()
	if err != nil {
		panic(err)
	}
	return fileKey(v)
}
