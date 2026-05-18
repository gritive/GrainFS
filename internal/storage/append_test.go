package storage

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func newTestLocalBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	t.Cleanup(func() { _ = b.Close() })
	if err := b.CreateBucket(context.Background(), "test"); err != nil {
		t.Fatalf("CreateBucket: %v", err)
	}
	return b
}

func TestAppendObjectRejectsMismatchedOffset(t *testing.T) {
	b := newTestLocalBackend(t)
	ctx := context.Background()
	if _, err := b.AppendObject(ctx, "test", "k", 0, strings.NewReader("0123456789")); err != nil {
		t.Fatalf("initial: %v", err)
	}
	_, err := b.AppendObject(ctx, "test", "k", 5, bytes.NewReader([]byte("xxx")))
	if !errors.Is(err, ErrAppendOffsetMismatch) {
		t.Fatalf("expected ErrAppendOffsetMismatch, got %v", err)
	}
}
