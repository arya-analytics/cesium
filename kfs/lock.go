package kfs

type lock struct {
	signal chan struct{}
}

func newLock() lock {
	return lock{signal: make(chan struct{}, 1)}
}

func (l lock) acquire() {
	// Wait for the lock to be released.
	<-l.signal
	// Reset the lock.
	l.signal = make(chan struct{}, 1)
}

func (l lock) release() {
	// If the lock is already released, we don't need to do anything.
	select {
	case l.signal <- struct{}{}:
	default:
	}
}
