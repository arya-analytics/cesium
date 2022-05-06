package query

import (
	"context"
	"fmt"
	"sync"
)

// Query is a container that represents a request for information.
//
// The parameters for Query are defined as a set of Options. Each option is an arbitrary string-value pair that
// provides information about how the Query should be executed.
//
// After assembly, the Query is passed to the Executor, which is responsible for parsing and executing the Query.
type Query interface {
	// Options holds the Options for the Query.
	Options
	// Context returns the context the caller executed the Query with.
	Context() context.Context
	// QExec executes the query.
	QExec() error
}

// New returns a new Query with empty Options that executes against the given Executor.
func New(exec Executor) Query {
	return defaultQuery{defaultOptions: make(defaultOptions), Executor: exec}
}

type defaultQuery struct {
	defaultOptions
	Executor
}

func (q defaultQuery) Context() context.Context {
	return GetContext(q)
}

func (q defaultQuery) QExec() error {
	return q.Executor.Exec(q)
}

// |||||| OPTIONS ||||||

// OptionKey is a type representing the key for a given option. OptionKey should be unique for each option.
// If writing a new option, ensure that the key is unique, or else unpredictable behavior may occur.
type OptionKey string

// Options is an interface for setting and getting options on a query.
type Options interface {
	// Get returns the option with the given key. If the option is not set, returns false as its second argument.
	// Unless an option is not required, it's recommended to use GetRequired instead.
	Get(key OptionKey) (interface{}, bool)
	// GetRequired returns the option with the given key. Panics if the option is not set.
	GetRequired(key OptionKey) interface{}
	// Set sets the option with the given key. Unless an option can be explicitly set multiple times, use SetOnce
	// instead.
	Set(key OptionKey, value interface{})
	// SetOnce sets the option with the given key. If the option is already set, it panics.
	SetOnce(key OptionKey, value interface{})
}

type defaultOptions map[OptionKey]interface{}

// Get implements Options.
func (o defaultOptions) Get(key OptionKey) (interface{}, bool) {
	v, ok := o[key]
	return v, ok
}

// GetRequired implements Options.
func (o defaultOptions) GetRequired(key OptionKey) interface{} {
	v, ok := o.Get(key)
	if !ok {
		panic(fmt.Sprintf("required option %s not set", key))
	}
	return v
}

// Set implements Options.
func (o defaultOptions) Set(key OptionKey, value interface{}) {
	o[key] = value
}

// SetOnce implements Options.
func (o defaultOptions) SetOnce(key OptionKey, value interface{}) {
	if _, ok := o[key]; ok {
		panic(fmt.Sprintf("option %s already set", key))
	}
	o[key] = value
}

// |||||| WAIT GROUP ||||||

const waitGroupOptKey = "waitGroup"

func setWaitGroup(q Query, wg *sync.WaitGroup) {
	q.Set(waitGroupOptKey, wg)
}

func WaitGroup(q Query) *sync.WaitGroup {
	return q.GetRequired(waitGroupOptKey).(*sync.WaitGroup)
}
