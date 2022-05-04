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

// Query is an interface that represents all available query variants cesium supports.
//
// Supported Variants:
//	- Create
//  - Retrieve
//  - Delete
//  - CreateChannel
//  - RetrieveChannel
//
type Query interface {
	Variant() interface{}
}

type query struct {
	exec    queryExecutor
	variant interface{}
	opts    map[queryOptKey]interface{}
}

type queryExecutor interface {
	exec(ctx context.Context, q query) error
}

// Variant returns the variant of the query, which is one of the variants listed in the Query interface.
func (q query) Variant() interface{} {
	return q.variant
}

// String returns a string representation of the query.
func (q query) String() string {
	return fmt.Sprintf("[QUERY] Variant %T | Opts %v", q.variant, q.opts)
}

func (q query) get(key queryOptKey) (interface{}, bool) {
	o, ok := q.opts[key]
	return o, ok
}

func (q query) set(key queryOptKey, value interface{}) {
	q.opts[key] = value
}

// |||||| CONSTRUCTORS |||||||

func newQuery(variant interface{}, exec queryExecutor) query {
	return query{
		variant: variant,
		opts:    make(map[queryOptKey]interface{}),
		exec:    exec,
	}
}

func newCreateChannel(exec queryExecutor) CreateChannel {
	return CreateChannel{query: newQuery(CreateChannel{}, exec)}
}

func newRetrieveChannel(exec queryExecutor) RetrieveChannel {
	return RetrieveChannel{query: newQuery(RetrieveChannel{}, exec)}
}

func newCreate(exec queryExecutor) Create {
	return Create{query: newQuery(Create{}, exec)}
}

func newRetrieve(exec queryExecutor) Retrieve {
	return Retrieve{query: newQuery(Retrieve{}, exec)}
}

func newDelete(exec queryExecutor) Delete {
	return Delete{query: newQuery(Delete{}, exec)}
}

// |||||| TYPE DEFINITIONS ||||||

// CreateChannel creates a new channel in the DB. See DB.NewCreate for more information.
type CreateChannel struct {
	query
}

// RetrieveChannel retrieves a channel from the DB. See DB.NewRetrieve for more information.
type RetrieveChannel struct {
	query
}

// CreateRequest is a request containing a set of segmentKV to write to the DB.
type CreateRequest struct {
	Segments []Segment
}

// CreateResponse contains any errors that occurred during the execution of the Create query.
type CreateResponse struct {
	Err error
}

// Error implements the StreamResponse interface.
func (r CreateResponse) Error() error {
	return r.Err
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

// |||||| CHANNEL ChannelKey ||||||

func setChannelKeys(q query, keys ...ChannelKey) {
	q.set(channelPKOptKey, keys)
}

func channelKeys(q query, errNotPresent bool) ([]ChannelKey, error) {
	pks, ok := getOpt[[]ChannelKey](q, channelPKOptKey)
	if !ok {
		if errNotPresent {
			return nil, newSimpleError(ErrInvalidQuery, "no channel pks provided to query")
		}
		return nil, nil
	}
	return pks, nil
}

func getChannelKey(q query) (ChannelKey, error) {
	pks, err := channelKeys(q, true)
	if err != nil {
		return 0, err
	}
	if len(pks) > 1 {
		return 0, newSimpleError(ErrInvalidQuery, "query only supports on channel pk")
	}
	return pks[0], nil
}

func getOpt[T any](q query, k queryOptKey) (T, bool) {
	opt, ok := q.get(k)
	ro, ok := opt.(T)
	return ro, ok
}

// WhereKey returns a query that will only return Channel that match the given primary LKey.
func (r RetrieveChannel) WhereKey(key ChannelKey) RetrieveChannel {
	r.set(channelPKOptKey, []ChannelKey{key})
	return r
}

// WhereChannels sets the channels to acquire a lock on for creation.
// The request stream will only accept segmentKV bound to channels with the given primary keys.
// If no keys are provided, will return an ErrInvalidQuery error.
func (c Create) WhereChannels(keys ...ChannelKey) Create {
	setChannelKeys(c.query, keys...)
	return c
}

// WhereChannels sets the channels to retrieve data for.
// If no keys are provided, will return an ErrInvalidQuery error.
func (r Retrieve) WhereChannels(keys ...ChannelKey) Retrieve {
	setChannelKeys(r.query, keys...)
	return r
}

// WhereChannels sets the channels to delete data from,
// If no keys are provided, will return an ErrInvalidQuery error.
func (d Delete) WhereChannels(keys ...ChannelKey) Delete {
	setChannelKeys(d.query, keys...)
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

// WhereTimeRange sets the time range to retrieve data from.
func (r Retrieve) WhereTimeRange(tr TimeRange) Retrieve {
	setTimeRange(r.query, tr)
	return r
}

// WhereTimeRange sets the time range to delete data from.
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

// Exec creates a new channel in the DB given the query parameters.
func (cc CreateChannel) Exec(ctx context.Context) (Channel, error) {
	if err := cc.exec.exec(ctx, cc.query); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](cc.query)
	return c, nil
}

// ExecN creates a set of N channels in the DB with identical parameters, as specified in the query.
func (cc CreateChannel) ExecN(ctx context.Context, n int) (channels []Channel, err error) {
	for i := 0; i < n; i++ {
		if err = cc.exec.exec(ctx, cc.query); err != nil {
			return nil, err
		}
		cs, _ := queryRecord[Channel](cc.query)
		channels = append(channels, cs)
	}
	return channels, nil
}

// Exec retrieves a channel from the DB given the query parameters.
func (r RetrieveChannel) Exec(ctx context.Context) (Channel, error) {
	if err := r.exec.exec(ctx, r.query); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](r.query)
	return c, nil
}

// Stream opens a stream
func (c Create) Stream(ctx context.Context) (chan<- CreateRequest, <-chan CreateResponse, error) {
	req := make(chan CreateRequest, 100)
	res := make(chan CreateResponse)
	s := &stream[CreateRequest, CreateResponse]{
		req:     req,
		res:     res,
		doneRes: CreateResponse{Err: io.EOF},
	}
	setStream(c.query, s)
	return s.req, res, c.exec.exec(ctx, c.query)
}

type RetrieveStream = stream[types.Nil, RetrieveResponse]

func (r Retrieve) Stream(ctx context.Context) (<-chan RetrieveResponse, error) {
	s := &RetrieveStream{res: make(chan RetrieveResponse)}
	setStream(r.query, s)
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
