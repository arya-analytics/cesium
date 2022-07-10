package channel

import (
	"github.com/arya-analytics/x/telem"
	"strconv"
)

type Key uint16

// String implements the fmt.Stringer interface.
func (k Key) String() string { return strconv.Itoa(int(k)) }

type Channel struct {
	Key      Key
	DataRate telem.DataRate
	DataType telem.DataType
}

// GorpKey implements the gorp.Entry interface.
func (c Channel) GorpKey() Key { return c.Key }

// SetOptions implements the gorp.Entry interface.
func (c Channel) SetOptions() (opts []interface{}) { return opts }
