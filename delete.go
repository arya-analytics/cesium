package cesium

import (
	"context"
	"github.com/arya-analytics/cesium/internal/query"
)

type Delete struct {
	query.Query
}

// |||||| CHANNEL ChannelKey ||||||

// WhereChannels sets the channels to delete data from,
// If no keys are provided, will return an ErrInvalidQuery error.
func (d Delete) WhereChannels(keys ...ChannelKey) Delete {
	setChannelKeys(d.Query, keys...)
	return d
}

// WhereTimeRange sets the time range to delete data from.
func (d Delete) WhereTimeRange(tr TimeRange) Delete {
	setTimeRange(d.Query, tr)
	return d
}

func (d Delete) Exec(ctx context.Context) error {
	query.SetContext(d.Query, ctx)
	return d.Query.QExec()
}
