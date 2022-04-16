package caesium

import (
	"context"
	"go/types"
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
	runner  queryRunner
	variant interface{}
	opts    map[queryOptKey]interface{}
}

type queryRunner interface {
	exec(ctx context.Context, q query) error
}

func (q query) retrieve(key queryOptKey) (interface{}, bool) {
	o, ok := q.opts[key]
	return o, ok
}

func (q query) set(key queryOptKey, value interface{}) {
	q.opts[key] = value
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

func newQuery(variant interface{}, exec queryRunner) query {
	return query{
		variant: variant,
		opts:    make(map[queryOptKey]interface{}),
		runner:  exec,
	}
}

func newCreateChannel(exec queryRunner) CreateChannel {
	return CreateChannel{query: newQuery(CreateChannel{}, exec)}
}

func newRetrieveChannel(exec queryRunner) RetrieveChannel {
	return RetrieveChannel{query: newQuery(RetrieveChannel{}, exec)}
}

func newCreate(exec queryRunner) Create {
	return Create{query: newQuery(Create{}, exec)}
}

func newRetrieve(exec queryRunner) Retrieve {
	return Retrieve{query: newQuery(Retrieve{}, exec)}
}

func newDelete(exec queryRunner) Delete {
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

func (cr CreateResponse) Error() error {
	return cr.Err
}

type Create struct {
	query
}

type RetrieveResponse struct {
	Segments []Segment
	Err      error
}

func (rr RetrieveResponse) Error() error {
	return rr.Err
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
		return TimeRange{}
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
	if err := cc.runner.exec(ctx, cc.query); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](cc.query)
	return c, nil
}

func (r RetrieveChannel) Exec(ctx context.Context) (Channel, error) {
	if err := r.runner.exec(ctx, r.query); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](r.query)
	return c, nil
}

func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, <-chan CreateResponse, error) {
	s := stream[CreateRequest, CreateResponse]{
		req: make(chan CreateRequest),
		res: make(chan CreateResponse),
	}
	setStream(c.query, s)
	return s.req, s.res, c.runner.exec(ctx, c.query)
}

func (r Retrieve) Stream(ctx context.Context) (<-chan RetrieveResponse, error) {
	s := stream[types.Nil, RetrieveResponse]{res: make(chan RetrieveResponse)}
	setStream[types.Nil, RetrieveResponse](r.query, s)
	return s.res, r.runner.exec(ctx, r.query)
}

func (d Delete) Exec(ctx context.Context) error {
	return d.runner.exec(ctx, d.query)
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

func (cc CreateChannel) WithDataRate(dr DataRate) CreateChannel {
	setDataRate(cc.query, dr)
	return cc
}

// |||| DENSITY ||||

func setDensity(q query, d Density) {
	q.set(densityOptKey, d)
}

func density(q query) (Density, bool) {
	d, ok := getOpt[Density](q, densityOptKey)
	return d, ok
}

func (cc CreateChannel) WithDensity(density Density) CreateChannel {
	setDensity(cc.query, density)
	return cc
}
