package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/channel"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/segment"
	"github.com/arya-analytics/x/confluence"
	"go.uber.org/zap"
	"sync"
)

type createParser struct {
	ctx       context.Context
	logger    *zap.Logger
	metrics   createMetrics
	wg        *sync.WaitGroup
	responses confluence.UnarySource[CreateResponse]
	channels  map[channel.Key]channel.Channel
	header    *kv.Header
}

func (c *createParser) parse(segments []Segment) []createOperation {
	var ops []createOperation
	for _, seg := range segments {
		op := createOperationUnary{
			ctx:         c.ctx,
			seg:         seg.Sugar(c.channels[seg.ChannelKey]),
			logger:      c.logger,
			kv:          c.header,
			metrics:     c.metrics,
			wg:          c.wg,
			UnarySource: c.responses,
		}
		c.metrics.segSize.Record(int(op.seg.UnboundedSize()))
		ops = append(ops, op)
	}
	return ops
}

type retrieveParser struct {
	responses *confluence.UnarySource[RetrieveResponse]
	logger    *zap.Logger
	metrics   retrieveMetrics
	wg        *sync.WaitGroup
}

func (r *retrieveParser) parse(ranges []*segment.Range) []retrieveOperation {
	var ops []retrieveOperation
	for _, rng := range ranges {
		for _, header := range rng.Headers {
			seg := header.Sugar(rng.Channel)
			seg.SetBounds(rng.Bound)
			ops = append(ops, retrieveOperationUnary{
				ctx:         context.Background(),
				seg:         seg,
				dataRead:    r.metrics.dataRead,
				wg:          r.wg,
				logger:      r.logger,
				UnarySource: r.responses,
			})
		}
	}
	return ops
}
