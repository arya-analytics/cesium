package kfs

import (
	"github.com/spf13/afero"
	"os"
)

// NewOS returns a new BaseFS that uses the os package.
func NewOS() BaseFS {
	return &osFS{}
}

// NewMem returns a new BaseFS that uses an afero memory filesystem.
func NewMem() BaseFS {
	return &memFS{fs: afero.NewMemMapFs()}
}

type osFS struct{}

func (o *osFS) Open(name string) (BaseFile, error) {
	return os.OpenFile(name, os.O_RDWR, 0666)

}

func (o *osFS) Create(name string) (BaseFile, error) {
	return os.Create(name)
}

func (o *osFS) Remove(name string) error {
	return os.Remove(name)
}

type memFS struct {
	fs afero.Fs
}

func (m *memFS) Open(name string) (BaseFile, error) {
	return m.fs.OpenFile(name, os.O_RDONLY, 0)
}

func (m *memFS) Create(name string) (BaseFile, error) {
	return m.fs.Create(name)
}

func (m *memFS) Remove(name string) error {
	return m.fs.Remove(name)
}
