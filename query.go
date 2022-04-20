package cesium

import (
	"context"
	"fmt"
	"go/types"
	"io"
)

// |||||| QUERY |||||||

type queryOptKey byte

const (
	channelPKOptKey queryOptKey = iota + 1
	timeRangeOptKey
	dataRateOptKey
	densityOptKey
	streamOptKey
	recordOptKey
)

type query struct {
	exec    queryExec
	variant interface{}
	opts    map[queryOptKey]interface{}
}

type queryExec interface {
	exec(ctx context.Context, q query) error
}

func (q query) retrieve(key queryOptKey) (interface{}, bool) {
	o, ok := q.opts[key]
	return o, ok
}

func (q query) set(key queryOptKey, value interface{}) {
	q.opts[key] = value
}

func (q query) String() string {
	return fmt.Sprintf("[QUERY] Variant %T | Opts %v", q.variant, q.opts)
}

type execFunc func(ctx context.Context, q query) error

type variantOpts struct {
	CreateChannel   execFunc
	RetrieveChannel execFunc
	Create          execFunc
	Retrieve        execFunc
	Delete          execFunc
}

func (q query) switchVariant(ctx context.Context, ops variantOpts) error {
	switch q.variant.(type) {
	case CreateChannel:
		return q.runVariant(ctx, ops.CreateChannel)
	case RetrieveChannel:
		return q.runVariant(ctx, ops.RetrieveChannel)
	case Create:
		return q.runVariant(ctx, ops.Create)
	case Retrieve:
		return q.runVariant(ctx, ops.Retrieve)
	case Delete:
		return q.runVariant(ctx, ops.Delete)
	}
	panic("invalid query variant received")
}

func (q query) runVariant(ctx context.Context, e execFunc) error {
	if e == nil {
		panic("received unknown variant")
	}
	return e(ctx, q)

}

// |||||| CONSTRUCTORS |||||||

func newQuery(variant interface{}, exec queryExec) query {
	return query{
		variant: variant,
		opts:    make(map[queryOptKey]interface{}),
		exec:    exec,
	}
}

func newCreateChannel(exec queryExec) CreateChannel {
	return CreateChannel{query: newQuery(CreateChannel{}, exec)}
}

func newRetrieveChannel(exec queryExec) RetrieveChannel {
	return RetrieveChannel{query: newQuery(RetrieveChannel{}, exec)}
}

func newCreate(exec queryExec) Create {
	return Create{query: newQuery(Create{}, exec)}
}

func newRetrieve(exec queryExec) Retrieve {
	return Retrieve{query: newQuery(Retrieve{}, exec)}
}

func newDelete(exec queryExec) Delete {
	return Delete{query: newQuery(Delete{}, exec)}
}

// |||||| TYPE DEFINITIONS ||||||

type CreateChannel struct {
	query
}

type RetrieveChannel struct {
	query
}

type CreateRequest struct {
	Segments []Segment
}

type CreateResponse struct {
	Err error
}

func (c CreateResponse) Error() error {
	return c.Err
}

type Create struct {
	query
}

type RetrieveResponse struct {
	Err      error
	Segments []Segment
}

func (r RetrieveResponse) Error() error {
	return r.Err
}

type Retrieve struct {
	query
}

type Delete struct {
	query
}

// |||||| CHANNEL ChannelPK ||||||

func setChannelPKs(q query, pks ...PK) {
	q.set(channelPKOptKey, pks)
}

func channelPKs(q query) []PK {
	pks, ok := getOpt[[]PK](q, channelPKOptKey)
	if !ok {
		return []PK{}
	}
	return pks
}

func channelPK(q query) (PK, error) {
	pks := channelPKs(q)
	if len(pks) == 0 {
		return PK{}, newSimpleError(ErrInvalidQuery, "no channel PKs provided to retrieve query")
	}
	if len(pks) > 1 {
		return PK{}, newSimpleError(ErrInvalidQuery, "query only supports on channel pk")
	}
	return pks[0], nil
}

func getOpt[T any](q query, k queryOptKey) (T, bool) {
	opt, ok := q.retrieve(k)
	ro, ok := opt.(T)
	return ro, ok
}

func (r RetrieveChannel) WherePK(pk PK) RetrieveChannel {
	setChannelPKs(r.query, pk)
	return r
}

func (c Create) WhereChannels(pks ...PK) Create {
	setChannelPKs(c.query, pks...)
	return c
}

func (r Retrieve) WhereChannels(pks ...PK) Retrieve {
	setChannelPKs(r.query, pks...)
	return r
}

func (d Delete) WhereChannels(pks ...PK) Delete {
	setChannelPKs(d.query, pks...)
	return d
}

// |||||| TIME RANGE ||||||

func setTimeRange(q query, tr TimeRange) {
	q.set(timeRangeOptKey, tr)
}

func timeRange(q query) TimeRange {
	tr, ok := getOpt[TimeRange](q, timeRangeOptKey)
	if !ok {
		return TimeRangeMax
	}
	return tr
}

func (r Retrieve) WhereTimeRange(tr TimeRange) Retrieve {
	setTimeRange(r.query, tr)
	return r
}

func (d Delete) WhereTimeRange(tr TimeRange) Delete {
	return d
}

// |||||| EXEC ||||||

func setQueryRecord[T any](q query, r T) {
	q.set(recordOptKey, r)
}

func queryRecord[T any](q query) (T, bool) {
	r, ok := getOpt[T](q, recordOptKey)
	return r, ok
}

func (cc CreateChannel) Exec(ctx context.Context) (Channel, error) {
	if err := cc.exec.exec(ctx, cc.query); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](cc.query)
	return c, nil
}

func (r RetrieveChannel) Exec(ctx context.Context) (Channel, error) {
	if err := r.exec.exec(ctx, r.query); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](r.query)
	return c, nil
}

func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, <-chan CreateResponse, error) {
	req := make(chan CreateRequest)
	res := make(chan CreateResponse)
	s := &stream[CreateRequest, CreateResponse]{
		req:     req,
		res:     res,
		doneRes: CreateResponse{Err: io.EOF},
	}
	setStream(c.query, s)
	return s.req, res, c.exec.exec(ctx, c.query)
}

func (r Retrieve) Stream(ctx context.Context) (<-chan RetrieveResponse, error) {
	s := &stream[types.Nil, RetrieveResponse]{res: make(chan RetrieveResponse)}
	setStream[types.Nil, RetrieveResponse](r.query, s)
	return s.res, r.exec.exec(ctx, r.query)
}

func (d Delete) Exec(ctx context.Context) error {
	return d.exec.exec(ctx, d.query)
}

// |||||| CREATE CHANNEL OPTIONS ||||||

// |||| DATA RATE ||||

func setDataRate(q query, dr DataRate) {
	q.set(dataRateOptKey, dr)
}

func dataRate(q query) (DataRate, bool) {
	dr, ok := getOpt[DataRate](q, dataRateOptKey)
	return dr, ok

}

func (cc CreateChannel) WithRate(dr DataRate) CreateChannel {
	setDataRate(cc.query, dr)
	return cc
}

// |||| DENSITY ||||

func setDensity(q query, d DataType) {
	q.set(densityOptKey, d)
}

func density(q query) (DataType, bool) {
	d, ok := getOpt[DataType](q, densityOptKey)
	return d, ok
}

func (cc CreateChannel) WithType(dt DataType) CreateChannel {
	setDensity(cc.query, dt)
	return cc
}
