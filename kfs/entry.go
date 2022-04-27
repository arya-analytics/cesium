package kfs

type entry struct {
	lock
	file File
}

func newEntry(f File) entry {
	return entry{lock: newLock(), file: f}
}
