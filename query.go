package caesium

import "context"

// |||||| QUERY |||||||

type queryOptKey byte

const (
	channelPKOptKey queryOptKey = iota + 1
	timeRangeOptKey
	dataRateOptKey
	densityOptKey
	streamOptKey
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

func (q query) switchVariant(ops struct {
	CreateChannel   func(query)
	RetrieveChannel func(query)
	Create          func(query)
	Retrieve        func(query)
	Delete          func(query)
}) {
	switch q.variant.(type) {
	case CreateChannel:
		ops.CreateChannel(q)
	case RetrieveChannel:
		ops.RetrieveChannel(q)
	case Create:
		ops.Create(q)
	case Retrieve:
		ops.Retrieve(q)
	case Delete:
		ops.Delete(q)
	}
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
	Error error
}

type Create struct {
	query
}

type RetrieveResponse struct {
	Segments []Segment
	Error    error
}

type Retrieve struct {
	query
}

type Delete struct {
	query
}

// |||||| CHANNEL PK ||||||

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

func (c CreateChannel) Exec(ctx context.Context) (Channel, error) {
	return Channel{}, nil
}

// |||||| RETRIEVE CHANNEL ||||||

func (r RetrieveChannel) Exec(ctx context.Context) (Channel, error) {
	return Channel{}, nil
}

type stream[T any] struct {
	values chan T
	errors chan error
}

func setStream[T any](q query, s stream[T]) {
	q.set(streamOptKey, s)
}

func getStream[T](q query) stream[T] {
	s, ok := getOpt[stream[T]](q, streamOptKey)
	if !ok {
		return stream[T]{}
	}
	return s
}

func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, error) {
	s := stream[CreateRequest]{
		values: make(chan CreateRequest),
		errors: make(chan error),
	}
	setStream(c.query, s)
	return s.values, c.runner.exec(ctx, c.query)
}

func (c Create) Errors() <-chan error {
	return getStream(c.query).errors
}

func (r Retrieve) Stream(ctx context.Context) (<-chan RetrieveResponse, error) {
	s := stream[RetrieveResponse]{values: make(chan RetrieveResponse)}
	setStream(r.query, s)
	return s.values, r.runner.exec(ctx, r.query)
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

func (c CreateChannel) WithDataRate(dr DataRate) CreateChannel {
	setDataRate(c.query, dr)
	return c
}

// |||| DENSITY ||||

func setDensity(q query, d Density) {
	q.set(densityOptKey, d)
}

func density(q query) (Density, bool) {
	d, ok := getOpt[Density](q, densityOptKey)
	return d, ok
}

func (c CreateChannel) WithDensity(density Density) CreateChannel {
	setDensity(c.query, density)
	return c
}
