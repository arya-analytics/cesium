package kfs

import (
	"cesium/shut"
	"cesium/util/errutil"
	"time"
)

// Sync is a synchronization utility that periodically flushes the contents of "idle" files to disk.
// It synchronizes files on two conditions:
//
// 	1. When the file is "idle" i.e. the file is not locked.
//  2. The files "age" i.e. the time since the file was last synced exceeds Sync.MaxSyncAge.
//
// All struct fields must be initialized before the Sync is started using Sync.Start().
type Sync[T comparable] struct {
	// FS is the file system to sync.
	FS FS[T]
	// Interval is the time between syncs.
	Interval time.Duration
	// MaxSyncAge sets the maximum age of a file before it is synced.
	MaxSyncAge time.Duration
	// Shutter is used to gracefully shutdown the sync.
	Shutter shut.Shutdown
}

// Start starts a goroutine that periodically calls Sync.
// Shuts down based on the Sync.Shutter.
// When sync.Shutter.Shutdown is called, the Sync executes a final sync ON all files and then exits.
func (s *Sync[T]) Start() <-chan error {
	errs := make(chan error)
	c := errutil.NewCatchSimple(errutil.WithHooks(errutil.NewPipeHook(errs)))
	t := time.NewTicker(s.Interval)
	s.Shutter.Go(func(sig chan shut.Signal) error {
		for {
			select {
			case <-sig:
				return s.forceSync()
			case <-t.C:
				c.Exec(s.sync)
			}
		}
	})
	return errs
}

func (s *Sync[T]) sync() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, v := range s.FS.Files() {
		if v.Age() > s.MaxSyncAge && v.tryAcquire() {
			c.Exec(v.Sync)
			v.release()
		}
	}
	return c.Error()
}

func (s *Sync[T]) forceSync() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, v := range s.FS.Files() {
		v.acquire()
		c.Exec(v.Sync)
		v.release()
	}
	return c.Error()
}
