// Copyright 2021 the Cesium authors. Sequential rights reserved..

package kfs

import (
	"fmt"
	"go.uber.org/zap"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FS wraps a file system (fs.FS) and exposes it as a simplified key(int):file(File) pair interface. FS is goroutine-safe, and uses a
// system of locks to ensure that concurrent accesses to the same file are serialized.
type FS[T comparable] interface {
	// Acquire acquires signal on file for reading and writing by its primary key. If the file does not exist,
	// creates a new file. Blocks until the signal is acquired. Release must be called to Release the signal.
	Acquire(key T) (File[T], error)
	// Release releases a file. Release is idempotent, and can be called even if the file was never acquired.
	Release(key T)
	// Close closes a file. Close is idempotent, and can be called even if the file was previously closed.
	// It's recommended that Close is called at a specified interval to ensure that all files are closed.
	// See Sync for a convenient way to do this.
	Close(key T) error
	// Remove acquires a file and then deletes it.
	Remove(key T) error
	// RemoveAll removes all files in the FS.
	RemoveAll() error
	// Metrics returns a snapshot of the current Metrics for the file system.
	Metrics() Metrics
	// Files returns a snapshot of the current files in the FS.
	Files() map[T]File[T]
}

// File is a file in the FS. It implements:
//
//		io.ReaderAt
//		io.ReadWriteCloser
//		io.Seeker
//
type File[T comparable] interface {
	Key() T
	BaseFile
	FileLock
	FileSync
}

type FileLock interface {
	Acquire()
	Release()
	TryAcquire() bool
}

type FileSync interface {
	// Age returns how much time has passed since the file was last sync to storage.
	Age() time.Duration
}

// BaseFS represents a file system that kfs.FS can wrap.
// Methods should behave the same as in the os package.
type BaseFS interface {
	Remove(name string) error
	Open(name string) (BaseFile, error)
	Create(name string) (BaseFile, error)
	Stat(name string) (os.FileInfo, error)
	Mkdir(name string, perm os.FileMode) error
}

type BaseFile interface {
	io.ReaderAt
	io.ReadWriteCloser
	io.Seeker
	// Sync syncs the file to the FS (os.File.sync).
	Sync() error
}

// New creates a new FS in the specified directory. If the directory does not exist, it will be created.
func New[T comparable](dirname string, opts ...Option) (FS[T], error) {
	o := newOptions(opts...)
	fs := &defaultFS[T]{
		dirname: dirname,
		options: *o,
		metrics: newMetrics(o.experiment),
		entries: make(map[T]File[T]),
	}
	return fs, fs.prep()
}

type defaultFS[T comparable] struct {
	options
	dirname string
	mu      sync.RWMutex
	metrics Metrics
	entries map[T]File[T]
}

// Acquire implements FS.
func (fs *defaultFS[T]) Acquire(key T) (File[T], error) {
	fs.logger.Debug("kfs acquiring file", zap.Any("key", key))
	sw := fs.metrics.Acquire.Stopwatch()
	sw.Start()
	defer sw.Stop()
	fs.mu.Lock()
	e, ok := fs.entries[key]
	if ok {
		// We need to unlock the mutex before we Acquire the Lock on the file,
		// so another goroutine can Release it.
		fs.mu.Unlock()
		e.Acquire()
		fs.logger.Debug("kfs acquired file",
			zap.Any("key", key),
			zap.Duration("duration", sw.Elapsed()),
		)
		return e, nil
	}
	f, err := fs.newEntry(key)
	fs.mu.Unlock()
	if err != nil {
		fs.logger.Error("kfs failed to acquire file", zap.Any("key", key), zap.Error(err))
	} else {
		fs.logger.Debug("kfs opened and acquired file",
			zap.Any("key", key),
			zap.Duration("duration", sw.Elapsed()),
		)
	}
	return f, err
}

// Release implements FS.
func (fs *defaultFS[T]) Release(key T) {
	fs.logger.Debug("kfs releasing file", zap.Any("key", key))
	sw := fs.metrics.Release.Stopwatch()
	sw.Start()
	defer sw.Stop()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if e, ok := fs.entries[key]; ok {
		fs.logger.Debug("kfs released file", zap.Any("key", key))
		e.Release()
	} else {
		fs.logger.Warn("kfs releasing file that does not exist", zap.Any("key", key))
	}
}

// Remove implements FS.
func (fs *defaultFS[T]) Remove(key T) error {
	fs.logger.Debug("kfs removing file", zap.Any("key", key))
	sw := fs.metrics.Delete.Stopwatch()
	sw.Start()
	defer sw.Stop()
	// Need to make sure other goroutines are done with the file before deleting it.
	if _, err := fs.Acquire(key); err != nil {
		return err
	}
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.entries, key)
	err := fs.baseFS.Remove(fs.path(key))
	if err != nil {
		fs.logger.Error("kfs failed to remove file", zap.Any("key", key), zap.Error(err))
	}
	return err
}

// Close implements FS.
func (fs *defaultFS[T]) Close(pk T) error {
	fs.logger.Debug("kfs closing file", zap.Any("key", pk))
	sw := fs.metrics.Close.Stopwatch()
	sw.Start()
	defer sw.Stop()
	fs.mu.Lock()
	defer fs.mu.Unlock()
	e, ok := fs.entries[pk]
	if !ok {
		return nil
	}
	e.Acquire()
	if err := e.Close(); err != nil {
		fs.logger.Error("kfs failed to close file", zap.Any("key", pk), zap.Error(err))
		return err
	}
	delete(fs.entries, pk)
	return nil
}

// RemoveAll implements FS.
func (fs *defaultFS[T]) RemoveAll() error {
	fs.logger.Debug("kfs removing all files")
	for pk := range fs.entries {
		if err := fs.Close(pk); err != nil {
			return err
		}
		if err := fs.Remove(pk); err != nil {
			return err
		}
	}
	return nil
}

// Metrics implements FS.
func (fs *defaultFS[T]) Metrics() Metrics {
	return fs.metrics
}

// Files implements FS. Note: does not return a copy. Do not modify the returned map.
func (fs *defaultFS[T]) Files() map[T]File[T] {
	return fs.entries
}

func (fs *defaultFS[T]) name(key T) string {
	return fmt.Sprint(key) + fs.suffix
}

func (fs *defaultFS[T]) path(key T) string {
	return filepath.Join(fs.dirname, fs.name(key))
}

func (fs *defaultFS[T]) newEntry(key T) (File[T], error) {
	f, err := fs.openOrCreate(key)
	if err != nil {
		return nil, err
	}
	e := newEntry(key, f)
	fs.entries[key] = e
	return e, nil
}

func (fs *defaultFS[T]) openOrCreate(key T) (BaseFile, error) {
	p := fs.path(key)
	f, err := fs.baseFS.Open(p)
	if err == nil || !os.IsNotExist(err) {
		return f, err
	}
	return fs.baseFS.Create(p)
}

func (fs *defaultFS[T]) prep() error {
	_, err := os.Stat(fs.dirname)
	if os.IsNotExist(err) {
		return fs.baseFS.Mkdir(fs.dirname, fs.dirPerms)
	}
	return err
}
