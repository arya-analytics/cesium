package channel

import "github.com/arya-analytics/x/telem"

type Key uint16

type Channel struct {
	Key      Key
	DataRate telem.DataRate
	DataType telem.DataType
}
