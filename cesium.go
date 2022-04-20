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

	// |||| PERSIST ||||

	kve := pebbleKV{DB: pdb}
	pst := newPersist(NewKFS(NewOS(filepath.Join(dirname, "cesium"))))

	// |||| BATCH QUEUE ||||

	bRunner := batchRunner{
		persist: pst,
		batch:   batchSet{retrieveBatch{}, createBatch{}},
	}

	batchQueue := newQueue(bRunner.exec)
	go batchQueue.tick()

	// |||| RUNNER ||||

	skv := newSegmentKV(kve)
	ckv := newChannelKV(kve)
	fa := newFileAllocate(skv)

	crSvc := newCreateChannelRunService(ckv)
	rcSvc := newRetrieveChannelRunService(ckv)

	cr := newCreateRunService(fa, skv, ckv)
	rc := newRetrieveRunService(skv)

	runner := &run{
		batchQueue: batchQueue.ops,
		services:   []runService{crSvc, rcSvc, cr, rc},
	}

	return &db{
		dirname: dirname,
		opts:    newOptions(opts...),
		kve:     kve,
		runner:  runner,
	}, nil
}

type db struct {
	dirname string
	opts    *options
	runner  *run
	kve     kvEngine
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
