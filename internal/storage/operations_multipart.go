package storage

import (
	"context"
	"io"
)

func (o *Operations) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error) {
	return o.backend.CreateMultipartUpload(ctx, bucket, key, contentType)
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
