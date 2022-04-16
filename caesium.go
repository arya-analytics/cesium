package caesium

import (
	"caesium/kv"
	"github.com/cockroachdb/pebble"
	"path/filepath"
)

// |||| DB ||||

type DB interface {
	NewCreate() Create
	NewRetrieve() Retrieve
	NewDelete() Delete
	NewCreateChannel() CreateChannel
	NewRetrieveChannel() RetrieveChannel
	Close() error
}

func New(dirname string, opts ...Option) (DB, error) {
	pdb, err := pebble.Open(filepath.Join(dirname, "db"), &pebble.Options{})
	if err != nil {
		return nil, err
	}
	kve := kv.PebbleEngine{DB: pdb}
	return &db{dirname: dirname, opts: newOptions(opts...), kve: kve}, nil
}

type db struct {
	dirname string
	opts    *options
	kve     kv.Engine
}

func (d *db) NewCreate() Create {
	return newCreate()
}

func (d *db) NewRetrieve() Retrieve {
	return newRetrieve()
}

func (d *db) NewDelete() Delete {
	return newDelete()
}

func (d *db) NewCreateChannel() CreateChannel {
	return newCreateChannel()
}

func (d *db) NewRetrieveChannel() RetrieveChannel {
	return newRetrieveChannel()
}

func (d *db) Close() error {
	return d.kve.Close()
}

// |||| OPTIONS ||||

type Option func(*options)

type options struct{}

func newOptions(opts ...Option) *options {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}
