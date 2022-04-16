package caesium

import "context"

// |||||| QUERY |||||||

type optKey string

type query struct {
	opts map[optKey]interface{}
}

func (q query) retrieve(key optKey) (interface{}, bool) {
	o, ok := q.opts[key]
	return o, ok
}

func (q query) set(key optKey, value interface{}) {
	q.opts[key] = value
}

// |||||| CONSTRUCTORS |||||||

func newQuery() query {
	return query{
		opts: make(map[optKey]interface{}),
	}
}

func newCreateChannel() CreateChannel {
	return CreateChannel{query: newQuery()}
}

func newRetrieveChannel() RetrieveChannel {
	return RetrieveChannel{query: newQuery()}
}

func newDelete() Delete {
	return Delete{query: newQuery()}
}

func newCreate() Create {
	return Create{query: newQuery()}
}

func newRetrieve() Retrieve {
	return Retrieve{query: newQuery()}
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

const channelPKOptKey optKey = "pk"

func setChannelPKs(q query, pks ...PK) {
	q.set(channelPKOptKey, pks)
}

func ChannelPKs(q query) []PK {
	pks, ok := getOpt[[]PK](q, channelPKOptKey)
	if !ok {
		return []PK{}
	}
	return pks
}

func getOpt[T any](q query, k optKey) (T, bool) {
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

const timeRangeOptKey optKey = "tr"

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

func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, error) {
	return nil, nil
}

func (c Create) Errors() <-chan CreateResponse {
	return nil
}

func (r Retrieve) Stream(ctx context.Context) <-chan RetrieveResponse {
	return nil
}

func (d Delete) Exec(ctx context.Context) error {
	return nil
}

// |||||| CREATE CHANNEL OPTIONS ||||||

// |||| DATA RATE ||||

const dataRateOptKey optKey = "dr"

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

const densityOptKey optKey = "ds"

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
