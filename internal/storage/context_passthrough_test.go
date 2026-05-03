package storage

import (
	"context"
	"io"
	"strings"
	"testing"
)

type contextRecorderBackend struct {
	ctx context.Context
}

func (b *contextRecorderBackend) CreateBucket(ctx context.Context, bucket string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) HeadBucket(ctx context.Context, bucket string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) DeleteBucket(ctx context.Context, bucket string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) ListBuckets(ctx context.Context) ([]string, error) {
	b.ctx = ctx
	return nil, nil
}
func (b *contextRecorderBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*Object, error) {
	b.ctx = ctx
	return &Object{Key: key}, nil
}
func (b *contextRecorderBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *Object, error) {
	b.ctx = ctx
	return io.NopCloser(strings.NewReader("ok")), &Object{Key: key, Size: 2}, nil
}
func (b *contextRecorderBackend) HeadObject(ctx context.Context, bucket, key string) (*Object, error) {
	b.ctx = ctx
	return &Object{Key: key}, nil
}
func (b *contextRecorderBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*Object, error) {
	b.ctx = ctx
	return nil, nil
}
func (b *contextRecorderBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*Object) error) error {
	b.ctx = ctx
	return nil
}
func (b *contextRecorderBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*MultipartUpload, error) {
	b.ctx = ctx
	return &MultipartUpload{UploadID: "u", Bucket: bucket, Key: key}, nil
}
func (b *contextRecorderBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*Part, error) {
	b.ctx = ctx
	return &Part{PartNumber: partNumber}, nil
}
func (b *contextRecorderBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []Part) (*Object, error) {
	b.ctx = ctx
	return &Object{Key: key}, nil
}
func (b *contextRecorderBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	b.ctx = ctx
	return nil
}

func TestSwappableBackend_ForwardsContext(t *testing.T) {
	rec := &contextRecorderBackend{}
	sb := NewSwappableBackend(rec)
	ctx := context.WithValue(context.Background(), testContextKey{}, "caller")

	if _, err := sb.PutObject(ctx, "b", "k", strings.NewReader("x"), "text/plain"); err != nil {
		t.Fatal(err)
	}
	if rec.ctx != ctx {
		t.Fatalf("wrapped backend got %p, want %p", rec.ctx, ctx)
	}
}

type testContextKey struct{}
