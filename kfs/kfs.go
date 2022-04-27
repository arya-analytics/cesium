// Copyright 2021 the Cesium authors. All rights reserved..

package kfs

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type File interface {
	io.ReaderAt
	io.Seeker
	io.Reader
	io.Writer
	io.Closer
}

// FS wraps a file system (fs.FS) and exposes it as a simplified key(int):file(File) pair interface. FS is goroutine-safe, and uses a
// system of locks to ensure that concurrent accesses to the same file are serialized.
type FS interface {
	// Acquire acquires signal on file for reading and writing by its primary key. If the file does not exist,
	// creates a new file. Blocks until the signal is acquired. Release must be called to Release the signal.
	Acquire(pk int) (File, error)
	// Release releases a file. Release is idempotent, and can be called even if the file was never acquired.
	Release(pk int)
	// Remove acquires a file and then deletes it.
	Remove(pk int) error
	// RemoveAll removes all files in the FS.
	RemoveAll() error
	// Metrics returns a snapshot of the current metrics for the file system.
	Metrics() Metrics
}

// BaseFS represents a file system that kfs.FS can wrap.
type BaseFS interface {
	Remove(name string) error
	Open(name string) (File, error)
	Create(name string) (File, error)
}

func New(root string, opts ...Option) FS {
	o := newOptions(opts...)
	return &defaultFS{
		root:    root,
		options: *o,
		metrics: newMetrics(o.experiment),
		entries: make(map[int]entry),
	}
}

type defaultFS struct {
	options
	root    string
	mu      sync.Mutex
	metrics Metrics
	entries map[int]entry
}

// Acquire implements FS.
func (fs *defaultFS) Acquire(pk int) (File, error) {
	fs.metrics.Acquire.Start()
	defer fs.metrics.Acquire.Stop()
	fs.mu.Lock()
	e, ok := fs.entries[pk]
	if ok {
		// We need to unlock the mutex before we acquire the lock on the file,
		// so another goroutine can release it.
		fs.mu.Unlock()
		e.acquire()
		return e.file, nil
	}
	f, err := fs.newEntry(pk)
	fs.mu.Unlock()
	return f, err
}

// Release implements FS.
func (fs *defaultFS) Release(pk int) {
	fs.metrics.Release.Start()
	defer fs.metrics.Release.Stop()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if e, ok := fs.entries[pk]; ok {
		e.release()
	}
}

// Remove implements FS.
func (fs *defaultFS) Remove(pk int) error {
	fs.metrics.Delete.Start()
	defer fs.metrics.Delete.Stop()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	e, ok := fs.entries[pk]
	if !ok {
		return nil
	}
	// Need to make sure other goroutines are done with the file before deleting it.
	e.acquire()
	// Close the file
	if err := e.file.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
		return err
	}
	delete(fs.entries, pk)
	return fs.baseFS.Remove(fs.path(pk))
}

// RemoveAll implements FS.
func (fs *defaultFS) RemoveAll() error {
	for pk := range fs.entries {
		if err := fs.Remove(pk); err != nil {
			return err
		}
	}
	return nil
}

// Metrics implements FS.
func (fs *defaultFS) Metrics() Metrics {
	return fs.metrics
}

func (fs *defaultFS) name(pk int) string {
	return strconv.Itoa(pk) + fs.suffix
}

func (fs *defaultFS) path(pk int) string {
	return filepath.Join(fs.root, fs.name(pk))
}

func (fs *defaultFS) newEntry(pk int) (File, error) {
	f, err := fs.openOrCreate(pk)
	if err != nil {
		return nil, err
	}
	fs.entries[pk] = newEntry(f)
	return f, nil
}

func (fs *defaultFS) openOrCreate(pk int) (File, error) {
	p := fs.path(pk)
	f, err := fs.baseFS.Open(p)
	if err == nil || !os.IsNotExist(err) {
		return f, err
	}
	return fs.baseFS.Create(p)
}
