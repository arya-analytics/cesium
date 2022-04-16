package keyfs

import (
	"caesium/pk"
	"io"
	"sync"
)

// |||| FS |||||

type File interface {
	PK() pk.PK
	io.ReaderAt
	io.Reader
	io.Seeker
	io.Writer
	io.Closer
}

type FS interface {
	Acquire(pk pk.PK) (File, error)
	Release(pk pk.PK)
	Delete(pk pk.PK) error
}

type FileSource interface {
	OpenOrCreate(pk pk.PK) (File, error)
	Delete(pk pk.PK) error
}

type kfs struct {
	mu      sync.Mutex
	source  FileSource
	entries map[pk.PK]fly
}

func New(source FileSource) FS {
	return &kfs{source: source, entries: make(map[pk.PK]fly)}
}

func (fs *kfs) Acquire(pk pk.PK) (File, error) {
	fs.mu.Lock()
	e, ok := fs.entries[pk]
	if !ok {
		f, err := fs.openOrCreate(pk)
		if err != nil {
			return nil, err
		}
		fs.entries[pk] = newEntry(f)
		fs.mu.Unlock()
		return f, nil
	}
	fs.mu.Unlock()
	e.lock()
	return e.f, nil
}

func (fs *kfs) Release(pk pk.PK) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	e, ok := fs.entries[pk]
	if !ok {
		return
	}
	e.unlock()
}

func (fs *kfs) Delete(pk pk.PK) error {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	e, ok := fs.entries[pk]
	if !ok {
		return nil
	}
	e.lock()
	delete(fs.entries, pk)
	if err := e.f.Close(); err != nil {
		return err
	}
	return fs.source.Delete(pk)
}

func (fs *kfs) openOrCreate(pk pk.PK) (File, error) {
	f, err := fs.source.OpenOrCreate(pk)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// |||| POOL ||||

type fly struct {
	l chan struct{}
	f File
}

func newEntry(f File) fly {
	return fly{l: make(chan struct{}), f: f}
}

func (e fly) lock() {
	<-e.l
	e.l = make(chan struct{})
}

func (e fly) unlock() {
	select {
	case <-e.l:
	default:
		close(e.l)
	}
}
