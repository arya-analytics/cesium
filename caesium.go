package cesium

import (
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

func Open(dirname string, opts ...Option) (DB, error) {
	pdb, err := pebble.Open(filepath.Join(dirname, "db"), &pebble.Options{})
	if err != nil {
		return nil, err
	}
	kve := pebbleKV{DB: pdb}
	return &db{
		dirname: dirname,
		opts:    newOptions(opts...),
		runner: &runner{
			ckv: newChannelKV(kve),
			kve: kve,
			pst: newPersist(NewKFS(NewOS(filepath.Join(dirname, "cesium")))),
			skv: newSegmentKV(kve),
		},
	}, nil
}

type db struct {
	dirname string
	opts    *options
	runner  *runner
}

func (d *db) NewCreate() Create {
	return newCreate(d.runner)
}

func (d *db) NewRetrieve() Retrieve {
	return newRetrieve(d.runner)
}

func (d *db) NewDelete() Delete {
	return newDelete(d.runner)
}

func (d *db) NewCreateChannel() CreateChannel {
	return newCreateChannel(d.runner)
}

func (d *db) NewRetrieveChannel() RetrieveChannel {
	return newRetrieveChannel(d.runner)
}

func (d *db) Close() error {
	return d.runner.close()
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
