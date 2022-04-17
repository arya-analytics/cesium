package cesium

import (
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// |||||| KFS |||||||

type KeyFile interface {
	PK() PK
	io.ReaderAt
	io.Reader
	io.Seeker
	io.Writer
	io.Closer
}

type KFSSource interface {
	open(pk PK) (KeyFile, error)
	delete(pk PK) error
}

type KFS struct {
	mu      sync.Mutex
	source  KFSSource
	entries map[PK]fly
}

func NewKFS(source KFSSource) *KFS {
	return &KFS{source: source, entries: make(map[PK]fly)}
}

func (fs *KFS) Acquire(pk PK) (KeyFile, error) {
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

func (fs *KFS) Release(pk PK) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	e, ok := fs.entries[pk]
	if !ok {
		return
	}
	e.unlock()
}

func (fs *KFS) Delete(pk PK) error {
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
	return fs.source.delete(pk)
}

func (fs *KFS) openOrCreate(pk PK) (KeyFile, error) {
	f, err := fs.source.open(pk)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// |||| POOL ||||

type fly struct {
	l chan struct{}
	f KeyFile
}

func newEntry(f KeyFile) fly {
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

type keyFile struct {
	pk PK
}

func (k keyFile) PK() PK {
	return k.pk
}

// |||||| OS FILE SOURCE ||||||

type osFile struct {
	keyFile
	*os.File
}

type OSKFSSource struct {
	Root string
}

func NewOS(root string) KFSSource {
	return &OSKFSSource{Root: root}
}

func (kfs *OSKFSSource) delete(pk PK) error {
	return os.Remove(kfs.path(pk))
}

func (kfs *OSKFSSource) path(pk PK) string {
	return filepath.Join(kfs.Root, pk.String())
}

func (kfs *OSKFSSource) open(pk PK) (KeyFile, error) {
	f, err := os.Open(kfs.path(pk))
	if !kfs.exists(err) {
		f, err = kfs.create(pk)
	}
	return &osFile{keyFile: keyFile{pk: pk}, File: f}, err
}

func (kfs *OSKFSSource) exists(err error) bool {
	fsErr, ok := err.(*fs.PathError)
	return !(ok && fsErr.Err == syscall.ENOENT)
}

func (kfs *OSKFSSource) create(pk PK) (*os.File, error) {
	f, err := os.Create(kfs.path(pk))
	if err != nil {
		return nil, err
	}
	return f, nil
}
