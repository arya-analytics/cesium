package operation

import (
	"cesium/kfs"
	"context"
)

type Operation[F comparable] interface {
	// Context returns a context, that when canceled represents a forced abort of the operation.
	Context() context.Context
	// FileKey returns the key of the file to which the operation applies.
	FileKey() F
	// SendError sends an error to the operation. This is only used for IO errors.
	SendError(error)
	// Exec is called by Persist to execute the operation. The provided file will have the key returned by FileKey.
	// The operation has a lock on the file during this time, and is free to make any modifications.
	Exec(f kfs.File[F])
}
