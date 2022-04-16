package caesium

import (
	"caesium/kv"
	"caesium/persist/keyfs"
	"context"
)

type runner struct {
	kve kv.Engine
}

type operation interface {
	fileKey() PK
	exec(ctx context.Context, f keyfs.File) error
}

func (o *runner) exec(ctx context.Context, q query) error {
	return nil
}

func (o *runner) close() error {
	return o.kve.Close()
}
