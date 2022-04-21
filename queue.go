package cesium

import (
	"cesium/shut"
	log "github.com/sirupsen/logrus"
	"time"
)

type tickQueue struct {
	ops  chan operation
	exec func(sets []operation)
	shut shut.Shutter
}

const (
	queueDefaultSize            = 150
	queueDefaultTick            = DataRate(100)
	emptyCycleShutdownThreshold = 2
)

func newQueue(setRunner func([]operation), shut shut.Shutter) *tickQueue {
	return &tickQueue{
		ops:  make(chan operation, queueDefaultSize),
		exec: setRunner,
		shut: shut,
	}
}

func (q *tickQueue) goTick() {
	q.shut.Go(func(sig chan shut.Signal) error {
		var (
			t              = time.NewTicker(queueDefaultTick.Period().Duration())
			sd             = false
			numEmptyCycles = 0
		)
		defer t.Stop()
		for {
			select {
			case <-sig:
				log.Info("[cesium.tickQueue] shutting down")
				sd = true
			default:
			}
			ops := q.opSet(t)
			if len(ops) == 0 {
				if sd {
					numEmptyCycles++
					if numEmptyCycles > emptyCycleShutdownThreshold {
						return nil
					}
				}
				continue
			}
			log.Infof("[cesium.tickQueue] sending %v operations to batch", len(ops))
			q.exec(ops)
		}
	})
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
			return ops
		}
	}
}
