// Copyright 2021 the Cesium authors. All rights reserved..

package kfs

import (
	"cesium/alamos"
	"io"
	"os"
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
	// Delete acquires a file and then deletes it.
	Delete(pk int) error
	// Metrics returns a snapshot of the current metrics for the file system.
	Metrics() Metrics
}

// BaseFS represents a file system that kfs.FS can wrap. It's method signatures are compatible with the os
// package.
type BaseFS interface {
	Remove(name string) error
	Open(name string) (File, error)
	Create(name string) (File, error)
}

type Options struct {
	// BaseFS is the underlying file system.
	BaseFS BaseFS
	// Suffix is the suffix to append to the file name.
	Suffix string
	// Experiment can be used to collect metrics and debug issues on the FS.
	Experiment alamos.Experiment
}

func New(root string, baseFS BaseFS, opts Options) FS {
	return &defaultFS{
		root:    root,
		baseFS:  baseFS,
		opts:    opts,
		metrics: newMetrics(opts.Experiment),
	}
}

type defaultFS struct {
	root    string
	opts    Options
	baseFS  BaseFS
	mu      sync.RWMutex
	metrics Metrics
	entries map[int]entry
}

// Acquire implements FS.
func (fs *defaultFS) Acquire(pk int) (File, error) {

	fs.metrics.Acquire.Start()
	defer fs.metrics.Acquire.Stop()

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	e, ok := fs.entries[pk]
	if ok {
		e.acquire()
		return e.file, nil
	}
	return fs.newEntry(pk)
}

// Release implements FS.
func (fs *defaultFS) Release(pk int) {

	fs.metrics.Release.Start()
	defer fs.metrics.Release.Stop()

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	if e, ok := fs.entries[pk]; ok {
		e.release()
	}
}

// Delete implements FS.
func (fs *defaultFS) Delete(pk int) error {

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
	delete(fs.entries, pk)
	return fs.baseFS.Remove(fs.path(pk))
}

// Metrics implements FS.
func (fs *defaultFS) Metrics() Metrics {
	return fs.metrics
}

func (fs *defaultFS) name(pk int) string {
	return fs.opts.Suffix + strconv.Itoa(int(pk))
}

func (fs *defaultFS) path(pk int) string {
	return fs.root + "/" + fs.name(pk)
}

func (fs *defaultFS) newEntry(pk int) (File, error) {
	f, err := fs.openOrCreate(pk)
	if err != nil {
		return nil, err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fs.entries[pk] = newEntry(f)
	return f, nil
}

func (fs *defaultFS) openOrCreate(pk int) (File, error) {
	f, err := fs.baseFS.Open(fs.path(pk))
	if err == nil {
		return f, nil
	}
	if !os.IsNotExist(err) {
		return nil, err
	}
	return fs.baseFS.Create(fs.name(pk))
}
