package cesium

import (
	"context"
	"fmt"
	"github.com/arya-analytics/cesium/internal/binary"
	"github.com/arya-analytics/cesium/internal/kv"
	"github.com/arya-analytics/cesium/internal/query"
	"io"
)

type ChannelKey = int16

type Channel struct {
	Key      ChannelKey
	DataRate DataRate
	DataType DataType
	Active   bool
}

func ChannelKeys(channels []Channel) []ChannelKey {
	keys := make([]ChannelKey, len(channels))
	for i, channel := range channels {
		keys[i] = channel.Key
	}
	return keys
}

// |||||| KV ||||||

// Flush implements kv.Flusher.
func (c Channel) Flush(w io.Writer) error {
	return binary.Write(w, c)
}

// Load implements kv.Loader.
func (c *Channel) Load(r io.Reader) error {
	return binary.Read(r, c)
}

const channelKVPrefix = "chan"

func (c Channel) KVKey() []byte {
	return kv.CompositeKey(channelKVPrefix, c.Key)
}

// LKey implements lock.MapItem.
func (c Channel) LKey() ChannelKey {
	return c.Key
}

type channelKV struct {
	kv kv.KV
}

func (ckv channelKV) get(key ChannelKey) (c Channel, err error) {
	k := Channel{Key: key}.KVKey()
	b, err := ckv.kv.Get(k)
	if err != nil {
		return c, err
	}
	return c, kv.LoadBytes(b, &c)
}

func (ckv channelKV) getMultiple(keys ...ChannelKey) (cs []Channel, err error) {
	for _, pk := range keys {
		c, err := ckv.get(pk)
		if err != nil {
			return cs, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

func (ckv channelKV) setMultiple(cs ...Channel) error {
	for _, c := range cs {
		if err := ckv.set(c); err != nil {
			return err
		}
	}
	return nil
}

func (ckv channelKV) set(c Channel) error {
	return kv.Flush(ckv.kv, c.KVKey(), c)
}

func (ckv channelKV) getEach(keys ...ChannelKey) (cs []Channel, err error) {
	for _, pk := range keys {
		c, err := ckv.get(pk)
		if err != nil {
			return cs, err
		}
		cs = append(cs, c)
	}
	return cs, nil
}

// |||||| CREATE ||||||

// |||| QUERY ||||

// CreateChannel creates a new channel in the DB. See DB.NewCreate for more information.
type CreateChannel struct {
	query.Query
}

// Exec creates a new channel in the DB given the query.Query parameters.
func (cc CreateChannel) Exec(ctx context.Context) (Channel, error) {
	query.SetContext(cc.Query, ctx)
	if err := cc.Query.QExec(); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](cc.Query)
	return c, nil
}

// ExecN creates a set of N channels in the DB with identical parameters, as specified in the query.Query.
func (cc CreateChannel) ExecN(ctx context.Context, n int) (channels []Channel, err error) {
	query.SetContext(cc.Query, ctx)
	for i := 0; i < n; i++ {
		if err = cc.Query.QExec(); err != nil {
			return channels, err
		}
		cs, _ := queryRecord[Channel](cc.Query)
		channels = append(channels, cs)
	}
	return channels, nil
}

// |||| FACTORY ||||

type createChannelFactory struct {
	exec query.Executor
}

func (c createChannelFactory) New() CreateChannel {
	return CreateChannel{Query: query.New(c.exec)}
}

// |||| EXECUTOR ||||

type createChannelQueryExecutor struct {
	ckv     channelKV
	counter *kv.PersistedCounter
}

func (cr *createChannelQueryExecutor) Exec(q query.Query) (err error) {
	npk, err := cr.counter.Increment()
	if err != nil {
		return err
	}
	c := Channel{Key: ChannelKey(npk), DataRate: dataRate(q), DataType: density(q)}
	err = cr.ckv.set(c)
	setQueryRecord[Channel](q, c)
	return err
}

// |||||| RETRIEVE ||||||

// |||| QUERY ||||

// RetrieveChannel retrieves a channel from the DB. See DB.NewRetrieve for more information.
type RetrieveChannel struct {
	query.Query
}

// WhereKey returns a query.Query that will only return Channel that match the given primary LKey.
func (r RetrieveChannel) WhereKey(key ChannelKey) RetrieveChannel {
	setChannelKeys(r.Query, key)
	return r
}

// Exec retrieves a channel from the DB given the query.Query parameters.
func (r RetrieveChannel) Exec(ctx context.Context) (Channel, error) {
	query.SetContext(r.Query, ctx)
	if err := r.Query.QExec(); err != nil {
		return Channel{}, err
	}
	c, _ := queryRecord[Channel](r.Query)
	return c, nil
}

// |||| FACTORY ||||

type retrieveChannelFactory struct {
	exec query.Executor
}

func (r retrieveChannelFactory) New() RetrieveChannel {
	return RetrieveChannel{Query: query.New(r.exec)}
}

// |||||| HOOKS ||||||

type validateChannelKeysHook struct {
	ckv channelKV
}

func (vch validateChannelKeysHook) Exec(query query.Query) error {
	_, err := vch.ckv.getMultiple(channelKeys(query)...)
	return err
}

// |||| EXECUTOR ||||

type retrieveChannelQueryExecutor struct {
	ckv channelKV
}

func (rc *retrieveChannelQueryExecutor) Exec(q query.Query) error {
	cpk := getChannelKey(q)
	c, err := rc.ckv.get(cpk)
	setQueryRecord[Channel](q, c)
	return err
}

// |||||| OPTIONS ||||||

// |||| DATA RATE ||||

const dataRateOptKey query.OptionKey = "dr"

func setDataRate(q query.Query, dr DataRate) {
	q.Set(dataRateOptKey, dr)
}

func dataRate(q query.Query) DataRate {
	return q.GetRequired(dataRateOptKey).(DataRate)
}

func (cc CreateChannel) WithRate(dr DataRate) CreateChannel {
	setDataRate(cc.Query, dr)
	return cc
}

// |||| DENSITY ||||

const densityOptKey query.OptionKey = "den"

func setDensity(q query.Query, d DataType) {
	q.Set(densityOptKey, d)
}

func density(q query.Query) DataType {
	return q.GetRequired(densityOptKey).(DataType)
}

func (cc CreateChannel) WithType(dt DataType) CreateChannel {
	setDensity(cc.Query, dt)
	return cc
}

const recordOptKey query.OptionKey = "rec"

func setQueryRecord[T any](q query.Query, r T) {
	q.Set(recordOptKey, r)
}

func queryRecord[T any](q query.Query) (T, bool) {
	r, ok := q.Get(recordOptKey)
	return r.(T), ok
}

// |||| KEYS ||||

const channelKeysOptKey query.OptionKey = "cks"

func setChannelKeys(q query.Query, keys ...ChannelKey) {
	q.Set(channelKeysOptKey, keys)
}

func channelKeys(q query.Query) []ChannelKey {
	return q.GetRequired(channelKeysOptKey).([]ChannelKey)
}

func getChannelKey(q query.Query) ChannelKey {
	pks := channelKeys(q)
	if len(pks) > 1 {
		panic(fmt.Sprintf("query %s only supports on channel key", q))
	}
	return pks[0]
}
