package kfs

import (
	"github.com/spf13/afero"
	"os"
)

// memFS implements the BaseFS interface for a memory filesystem.
type memFS struct {
	fs afero.Fs
}

func NewMem() BaseFS {
	return &memFS{fs: afero.NewMemMapFs()}
}

func (m *memFS) Open(name string) (File, error) {
	return m.fs.OpenFile(name, os.O_RDONLY, 0)
}

func (m *memFS) Create(name string) (File, error) {
	return m.fs.Create(name)
}

func (m *memFS) Remove(name string) error {
	return m.fs.Remove(name)
}
