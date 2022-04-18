package cesium

import (
	"time"
)

type tickQueue struct {
	ops       chan operation
	setRunner func(...operation)
}

const (
	queueDefaultSize = 10
	queueDefaultTick = DataRate(100)
)

func newQueue(setRunner func(...operation)) *tickQueue {
	return &tickQueue{ops: make(chan operation, queueDefaultSize), setRunner: setRunner}
}

func (q *tickQueue) tick() {
	t := time.NewTicker(queueDefaultTick.Period().Duration())
	defer t.Stop()
	for {
		q.setRunner(q.opSet(t)...)
	}
}

func (q *tickQueue) opSet(t *time.Ticker) []operation {
	ops := make([]operation, 0, queueDefaultSize)
	for {
		select {
		case op := <-q.ops:
			ops = append(ops, op)
			if len(ops) >= queueDefaultSize {
				return ops
			}
		case <-t.C:
			if len(ops) > 0 {
				return ops
			}
		}
	}
}

type opSet struct {
	ops  []operation
	done chan struct{}
}

func (d opSet) wait() {
	c := 0
	for range d.done {
		c++
		if c >= len(d.ops) {
			return
		}
	}
}
