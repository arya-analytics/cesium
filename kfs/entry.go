package kfs

import (
	"time"
)

type entry struct {
	BaseFile
	lock
	ls time.Time
}

func (e *entry) Age() time.Duration {
	return time.Since(e.ls)
}

func (e *entry) Sync() error {
	e.ls = time.Now()
	return e.BaseFile.Sync()
}

func newEntry(f BaseFile) File {
	return &entry{
		lock:     newLock(),
		BaseFile: f,
		ls:       time.Now(),
	}
}
