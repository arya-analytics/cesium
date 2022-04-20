package cesium

import (
	log "github.com/sirupsen/logrus"
	"time"
)

type tickQueue struct {
	ops       chan operation
	setRunner func(sets []operation)
}

const (
	queueDefaultSize = 300
	queueDefaultTick = DataRate(100)
)

func newQueue(setRunner func([]operation)) *tickQueue {
	return &tickQueue{ops: make(chan operation, queueDefaultSize), setRunner: setRunner}
}

func (q *tickQueue) tick() {
	t := time.NewTicker(queueDefaultTick.Period().Duration())
	defer t.Stop()
	for {
		ops := q.opSet(t)
		log.Infof("[QUEUE] sending %v operations to batch", len(ops))
		q.setRunner(ops)
	}
}

func (q *tickQueue) opSet(t *time.Ticker) []operation {
	ops := make([]operation, 0, queueDefaultSize)
	for {
		select {
		case op := <-q.ops:
			//log.Infof("[QUEUE] received operation %s %v", op, len(ops))
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
