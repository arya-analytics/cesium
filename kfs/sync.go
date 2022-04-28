package kfs

import (
	"cesium/shut"
	"cesium/util/errutil"
	"time"
)

type Sync[T comparable] struct {
	FS         FS[T]
	Interval   time.Duration
	MaxSyncAge time.Duration
	Shutter    shut.Shutter
}

// GoTick starts a goroutine that periodically calls Sync.
// Shuts down based on the Sync.Shutter.
func (s *Sync[T]) GoTick() <-chan error {
	errs := make(chan error)
	c := errutil.NewCatchSimple(errutil.WithHooks(errutil.NewPipeHook(errs)))
	t := time.NewTicker(s.Interval)
	s.Shutter.Go(func(sig chan shut.Signal) error {
		for {
			select {
			case <-sig:
				return s.Sync()
			case <-t.C:
				c.Exec(s.Sync)
			}
		}
	})
	return errs
}

// Sync syncs the FS to the underlying storage.
func (s *Sync[T]) Sync() error {
	c := errutil.NewCatchSimple(errutil.WithAggregation())
	for _, v := range s.FS.Files() {
		if v.LastSync() > s.MaxSyncAge && v.Idle() {
			c.Exec(v.Sync)
		}
	}
	return c.Error()
}
