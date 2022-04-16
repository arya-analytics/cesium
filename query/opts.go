package query

import (
	"caesium/pk"
	"caesium/telem"
)

// |||| TIME RANGE ||||

const timeRangeOptKey optKey = "tr"

func setTimeRange(q Query, tr telem.TimeRange) {
	q.set(timeRangeOptKey, tr)
}

func TimeRange(q Query) telem.TimeRange {
	tr, ok := getOpt[telem.TimeRange](q, timeRangeOptKey)
	if !ok {
		return telem.TimeRange{}
	}
	return tr
}

// |||| CHANNEL PKS ||||

const channelPKOptKey optKey = "pk"

func setChannelPKs(q Query, pks ...pk.PK) {
	q.set(channelPKOptKey, pks)
}

func ChannelPKs(q Query) []pk.PK {
	pks, ok := getOpt[[]pk.PK](q, channelPKOptKey)
	if !ok {
		return []pk.PK{}
	}
	return pks
}

func getOpt[T any](q Query, k optKey) (T, bool) {
	opt, ok := q.retrieve(k)
	ro, ok := opt.(T)
	return ro, ok
}
