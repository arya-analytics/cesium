package caesium

import (
	"caesium/kv"
	"caesium/query"
	"github.com/cockroachdb/pebble"
	"path/filepath"
)

// |||| DB ||||

type DB interface {
	NewCreate() query.Create
	NewRetrieve() query.Retrieve
	NewDelete() query.Delete
	NewCreateChannel() query.CreateChannel
	NewRetrieveChannel() query.RetrieveChannel
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

func (d *db) NewCreate() query.Create {
	return query.NewCreate(d.kve)
}

func (d *db) NewRetrieve() query.Retrieve {
	return query.NewRetrieve()
}

func (d *db) NewDelete() query.Delete {
	return query.NewDelete()
}

func (d *db) NewCreateChannel() query.CreateChannel {
	return query.NewCreateChannel(d.kve)
}

func (d *db) NewRetrieveChannel() query.RetrieveChannel {
	return query.NewRetrieveChannel(d.kve)
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
