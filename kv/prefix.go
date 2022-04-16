package kv

type Prefix byte

func PrefixedKey(p Prefix, key []byte) []byte {
	return append([]byte{byte(p)}, key...)
}
