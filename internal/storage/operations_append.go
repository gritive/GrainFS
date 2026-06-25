package storage

import (
	"context"
	"io"
)

// AppendObject routes an S3 Express append through the cached operations plan,
// mirroring SetObjectTags/SetObjectACL: it resolves the optional AppendObjecter
// capability from the backend decorator chain and returns UnsupportedOperationError
// when no layer provides it. Keeping append on the Operations surface lets the HTTP
// handler stay backend-blind (single-path), like every other object op.
func (o *Operations) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*Object, error) {
	plan := o.planForCall()
	if plan.appender == nil {
		return nil, UnsupportedOperationError{Op: "AppendObject", Reason: UnsupportedReasonNoAdapter}
	}
	return plan.appender.AppendObject(ctx, bucket, key, expectedOffset, r)
}
