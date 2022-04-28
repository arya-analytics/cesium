package kfs

type lock struct {
	signal chan struct{}
}

func newLock() lock {
	return lock{signal: make(chan struct{}, 1)}
}

// acquire blocks until a lock is acquired.
func (l lock) acquire() {
	// Wait for the lock to be released.
	<-l.signal
	// Reset the lock.
	l.signal = make(chan struct{}, 1)
}

// tryAcquire attempts to acquire the lock.
// Returns true if the lock was acquired. If true is returned, the lock MUST be released after work is done.
func (l lock) tryAcquire() (acquired bool) {
	select {
	case <-l.signal:
		l.signal = make(chan struct{}, 1)
		acquired = true
	default:
	}
	return acquired
}

// release the lock.
func (l lock) release() {
	// If the lock is already released, we don't need to do anything.
	select {
	case l.signal <- struct{}{}:
	default:
	}
}
