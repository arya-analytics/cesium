package lock

type Lock struct {
	signal chan struct{}
}

func New() Lock {
	l := Lock{signal: make(chan struct{}, 1)}
	// lock is initially released.
	l.signal <- struct{}{}
	return l
}

// Acquire blocks until a Lock is acquired.
func (l Lock) Acquire() {
	// Wait for the Lock to be released.
	<-l.signal
	// Reset the Lock.
	l.signal = make(chan struct{}, 1)
}

// TryAcquire attempts to acquire the Lock.
// Returns true if the Lock was acquired. If true is returned, the Lock MUST be released after work is done.
func (l Lock) TryAcquire() (acquired bool) {
	select {
	case <-l.signal:
		l.signal = make(chan struct{}, 1)
		acquired = true
	default:
	}
	return acquired
}

// Release the Lock.
func (l Lock) Release() {
	// If the Lock is already released, we don't need to do anything.
	select {
	case l.signal <- struct{}{}:
	default:
	}
}
