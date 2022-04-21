package cesium

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

// |||||| kfs |||||||

type keyFile interface {
	PK() PK
	io.ReaderAt
	io.Reader
	io.Seeker
	io.Writer
	io.Closer
}

type kfsSource interface {
	open(pk PK) (keyFile, error)
	delete(pk PK) error
}

type KFS struct {
	mu      sync.Mutex
	source  kfsSource
	entries map[PK]fly
}

func NewKFS(source kfsSource) *KFS {
	return &KFS{source: source, entries: make(map[PK]fly)}
}

func (fs *KFS) Acquire(pk PK) (keyFile, error) {
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

func (fs *KFS) openOrCreate(pk PK) (keyFile, error) {
	f, err := fs.source.open(pk)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// |||| POOL ||||

type fly struct {
	l chan struct{}
	f keyFile
}

func newEntry(f keyFile) fly {
	return fly{l: make(chan struct{}, 1), f: f}
}

func (e fly) lock() {
	log.Debugf("[kfs] acquiring lock on file %s", e.f.PK())
	<-e.l
	log.Debugf("[kfs] acquired lock on file %s", e.f.PK())
	e.l = make(chan struct{}, 1)
}

func (e fly) unlock() {
	log.Debugf("[kfs] releasing lock on file %s", e.f.PK())
	select {
	case <-e.l:
	default:
		e.l <- struct{}{}
	}
	log.Debugf("[kfs] released lock on file %s", e.f.PK())
}

type baseKeyFile struct {
	pk PK
}

func (k baseKeyFile) PK() PK {
	return k.pk
}

// |||||| OS FILE SOURCE ||||||

type osFile struct {
	baseKeyFile
	*os.File
}

type OSKFSSource struct {
	Root string
}

func NewOS(root string) *KFS {
	return NewKFS(&OSKFSSource{Root: root})
}

func (kfs *OSKFSSource) delete(pk PK) error {
	return os.Remove(kfs.path(pk))
}

func (kfs *OSKFSSource) path(pk PK) string {
	return filepath.Join(kfs.Root, pk.String())
}

func (kfs *OSKFSSource) open(pk PK) (keyFile, error) {
	log.Infof("[kfs] opening file %s", pk)
	f, err := os.Open(kfs.path(pk))
	if !fileExists(err) {
		f, err = kfs.create(pk)
	}
	return &osFile{baseKeyFile: baseKeyFile{pk: pk}, File: f}, err
}

func fileExists(err error) bool {
	fsErr, ok := err.(*fs.PathError)
	if !ok {
		return true
	}
	return fsErr.Err == syscall.ENOENT
}

func (kfs *OSKFSSource) create(pk PK) (*os.File, error) {
	return os.Create(kfs.path(pk))
}

// |||||| AFERO FILE SOURCE ||||||

type aferoFile struct {
	baseKeyFile
	afero.File
}

type AferoKFSSource struct {
	Root string
	Fs   afero.Fs
}

func NewAfero(root string) *KFS {
	af := afero.NewMemMapFs()
	return NewKFS(&AferoKFSSource{Fs: af})
}

func (kfs *AferoKFSSource) delete(pk PK) error {
	return kfs.Fs.Remove(pk.String())
}

func (kfs *AferoKFSSource) open(pk PK) (keyFile, error) {
	log.Infof("[kfs] opening file %s", pk)
	f, err := kfs.Fs.Open(pk.String())
	if !fileExists(err) {
		f, err = kfs.create(pk)
	}
	return &aferoFile{baseKeyFile: baseKeyFile{pk: pk}, File: f}, err
}

func (kfs *AferoKFSSource) create(pk PK) (afero.File, error) {
	return kfs.Fs.Create(pk.String())
}
