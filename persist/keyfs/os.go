package keyfs

import (
	"caesium/pk"
	"io/fs"
	"os"
	"path/filepath"
	"syscall"
)

type OSFileSource struct {
	Root string
}

func NewOSFileSource(root string) FileSource {
	return &OSFileSource{Root: root}
}

type OSFile struct {
	pk pk.PK
	*os.File
}

func (f *OSFile) PK() pk.PK {
	return f.pk
}

func (fs *OSFileSource) path(pk pk.PK) string {
	return filepath.Join(fs.Root, pk.String())
}

func (fs *OSFileSource) OpenOrCreate(pk pk.PK) (File, error) {
	f, err := os.Open(fs.path(pk))
	if fileNotExists(err) {
		f, err = os.Create(fs.path(pk))
		if err != nil {
			return nil, err
		}
	}
	return &OSFile{pk: pk, File: f}, err
}

func fileNotExists(err error) bool {
	fsErr, ok := err.(*fs.PathError)
	return ok && fsErr.Err == syscall.ENOENT
}

func (fs *OSFileSource) Delete(pk pk.PK) error {
	return os.Remove(fs.path(pk))
}
