package cesium

import (
	"github.com/arya-analytics/cesium/internal/allocate"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/kfs"
)

type (
	fileKey    int16
	fileSystem = kfs.FS[fileKey]
	file       = kfs.File[fileKey]
)

const (
	// maxFileSize is the default maximum size of a cesium file.
	maxFileSize = allocate.DefaultMaxDescriptors
	// maxFileDescriptors is the default maximum number of file descriptors
	// cesium can open at a time.
	maxFileDescriptors = allocate.DefaultMaxSize
)

type fileCounter struct {
	kv.PersistedCounter
}

func newFileCounter(kve kv.KV, key []byte) (*fileCounter, error) {
	counter, err := kv.NewPersistedCounter(kve, key)
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
