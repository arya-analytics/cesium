package query

type OptKey string

type Query interface {
	Get(key OptKey) interface{}
	Set(key OptKey, value interface{})
}

type options map[OptKey]interface{}

func (o options) Get(key OptKey) interface{} {
	return o[key]
}

func (o options) Set(key OptKey, value interface{}) {
	o[key] = value
}

type defaultQuery struct {
	options
}

func New() Query {
	return defaultQuery{options: make(options)}
}
