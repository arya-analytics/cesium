package query

type optKey string

type Variant interface{}

type Query struct {
	opts map[optKey]interface{}
}

func (q Query) retrieve(key optKey) (interface{}, bool) {
	o, ok := q.opts[key]
	return o, ok
}

func (q Query) set(key optKey, value interface{}) {
	q.opts[key] = value
}
