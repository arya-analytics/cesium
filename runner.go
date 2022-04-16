package caesium

import (
	"caesium/persist/keyfs"
	"context"
)

type runner struct {
	ckv channelKV
}

type operation interface {
	fileKey() PK
	exec(ctx context.Context, f keyfs.File) error
}

func (r *runner) exec(ctx context.Context, q query) error {
	return q.switchVariant(ctx, variantOpts{
		CreateChannel:   r.ckv.exec,
		RetrieveChannel: r.ckv.exec,
	})
}

func (r *runner) close() error {
	return nil
}
