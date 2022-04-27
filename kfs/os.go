package kfs

import "os"

// osFS implements the BaseFS interface for the os filesystem.
type osFS struct{}

func (o *osFS) Open(name string) (File, error) {
	// open a file for reading and writing
	return os.OpenFile(name, os.O_RDWR, 0666)

}

func (o *osFS) Create(name string) (File, error) {
	return os.Create(name)
}

func (o *osFS) Remove(name string) error {
	return os.Remove(name)
}

func NewOSFS() BaseFS {
	return &osFS{}
}
