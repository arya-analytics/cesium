package queue

import (
	"cesium/shut"
	"time"
)

// Debounce is a simple, goroutine safe queue that flushes data to a channel on a timer or queue size threshold.
type Debounce[T any] struct {
	// Requests is the channel to send values to add to the queue.
	Requests chan []T
	// Responses is the channel to receive values from the queue.
	// Responses will be closed when the queue is closed.
	Responses chan []T
	// Shutdown is used to gracefully shut down the queue.
	Shutdown shut.Shutdown
	// Interval is the time between flushes.
	Interval time.Duration
	// Threshold is the maximum number of values to store in Debounce.
	// Debounce will flush when this threshold is reached, regardless of the Interval.
	Threshold int
}

const (
	emptyCycleShutdownCount = 5
)

// Start starts the queue.
func (d *Debounce[T]) Start() {
	d.Shutdown.Go(func(sig chan shut.Signal) error {
		var (
			t        = time.NewTicker(d.Interval)
			sd       = false
			numEmpty = 0
		)
		defer t.Stop()
		for {
			select {
			case <-sig:
				sd = true
			default:
			}
			values := d.fill(t)
			if len(values) == 0 {
				if sd {
					numEmpty++
					if numEmpty > emptyCycleShutdownCount {
						close(d.Responses)
						return nil
					}
				}
				continue
			}
			d.Responses <- values
		}
	})
}

func (d *Debounce[T]) fill(t *time.Ticker) []T {
	ops := make([]T, 0, d.Threshold)
	for {
		select {
		case requests := <-d.Requests:
			ops = append(ops, requests...)
			if len(ops) >= d.Threshold {
				return ops
			}
		case <-t.C:
			return ops
		}
	}
}
