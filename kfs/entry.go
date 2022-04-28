package kfs

import (
	"time"
)

type entry struct {
	BaseFile
	lock
	lastSync time.Time
}

func (e *entry) LastSync() time.Duration {
	return time.Since(e.lastSync)
}

func (e *entry) Sync() error {
	e.lastSync = time.Now()
	return e.BaseFile.Sync()
}

func newEntry(f BaseFile) File {
	return &entry{
		lock:     newLock(),
		BaseFile: f,
		lastSync: time.Now(),
	}
}
