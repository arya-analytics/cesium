package channel

import (
	"caesium/kv"
	"caesium/pk"
)

const Prefix kv.Prefix = 0x1

func Get(e kv.Engine, pk pk.PK) (c Channel, err error) {
	return kv.GetWithPrefixedPK[Channel](e, Prefix, pk, c)
}

func Set(e kv.Engine, c Channel) error {
	return kv.SetWithPrefixedPK[Channel](e, Prefix, c.PK, c)
}
