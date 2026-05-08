package storage

import (
	"context"
	"io"
	"time"
)

func (o *Operations) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error) {
	return o.backend.CreateMultipartUpload(ctx, bucket, key, contentType)
}

// SweepOrphanMultiparts walks the decorator stack for an OrphanMultipartSweeper
// and runs the sweep against entries whose mtime is at or before the cutoff.
// Returns a zero-value result when no sweeper is reachable (no-op for backends
// that don't own a multipart staging area). Returns the gate error verbatim
// when a RecoveryWriteGate sits between the facade and the sweeper, mirroring
// the policy that gated runtimes refuse mutation even on cleanup paths.
func (o *Operations) SweepOrphanMultiparts(ctx context.Context, before time.Time) (OrphanMultipartSweepResult, error) {
	for b := o.backend; b != nil; b = unwrapOperationBackend(b) {
		if g, blocksWrites := b.(*RecoveryWriteGate); blocksWrites {
			return OrphanMultipartSweepResult{}, g.err
		}
		if sw, ok := b.(OrphanMultipartSweeper); ok {
			return sw.SweepOrphanMultiparts(ctx, before)
		}
	}
	return OrphanMultipartSweepResult{}, nil
}

func (o *Operations) UploadPart(
	ctx context.Context,
	bucket, key, uploadID string,
	partNumber int,
	r io.Reader,
) (*Part, error) {
	return o.backend.UploadPart(ctx, bucket, key, uploadID, partNumber, r)
}

func (o *Operations) CompleteMultipartUpload(
	ctx context.Context,
	bucket, key, uploadID string,
	parts []Part,
) (*Object, error) {
	return o.backend.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
}

func (o *Operations) CompleteMultipartUploadWithResult(
	ctx context.Context,
	bucket, key, uploadID string,
	parts []Part,
) (*PutObjectResult, error) {
	previous, err := o.previousObject(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	obj, err := o.backend.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
	if err != nil {
		return nil, err
	}
	facts, err := mutationObjectFacts("CompleteMultipartUpload", obj)
	if err != nil {
		return nil, err
	}
	return &PutObjectResult{Object: facts, Previous: previous}, nil
}

func (o *Operations) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return o.backend.AbortMultipartUpload(ctx, bucket, key, uploadID)
}

func (o *Operations) ListMultipartUploads(ctx context.Context, bucket, prefix string, maxUploads int) ([]*MultipartUpload, error) {
	return o.backend.ListMultipartUploads(ctx, bucket, prefix, maxUploads)
}

func (o *Operations) ListParts(ctx context.Context, bucket, key, uploadID string, maxParts int) ([]Part, error) {
	return o.backend.ListParts(ctx, bucket, key, uploadID, maxParts)
}
