package serveruntime

import (
	"context"
	"errors"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
)

// fakeBackend records which bucket names were passed to each create path.
// It satisfies storage.Backend minimally — only the methods tested here are
// implemented; the rest panic to catch accidental calls.
type fakeBackend struct {
	storage.Backend // embed for unimplemented methods

	created       []string
	bypassCreated []string
	alreadyExists map[string]bool
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{alreadyExists: map[string]bool{}}
}

func (f *fakeBackend) CreateBucket(_ context.Context, bucket string) error {
	if f.alreadyExists[bucket] {
		return storage.ErrBucketAlreadyExists
	}
	f.created = append(f.created, bucket)
	f.alreadyExists[bucket] = true
	return nil
}

func (f *fakeBackend) CreateBucketBypassReserved(_ context.Context, bucket string) error {
	if f.alreadyExists[bucket] {
		return storage.ErrBucketAlreadyExists
	}
	f.bypassCreated = append(f.bypassCreated, bucket)
	f.alreadyExists[bucket] = true
	return nil
}

func TestSeedReservedBucket_UsesSeederWhenAvailable(t *testing.T) {
	fb := newFakeBackend()
	ctx := context.Background()

	if err := seedReservedBucket(ctx, fb, "_grainfs"); err != nil {
		t.Fatalf("seedReservedBucket(_grainfs): %v", err)
	}
	if err := seedReservedBucket(ctx, fb, "default"); err != nil {
		t.Fatalf("seedReservedBucket(default): %v", err)
	}

	if len(fb.bypassCreated) != 2 {
		t.Errorf("expected 2 bypass creates, got %v", fb.bypassCreated)
	}
	if len(fb.created) != 0 {
		t.Errorf("expected 0 regular creates, got %v", fb.created)
	}
}

func TestSeedReservedBucket_IdempotentOnAlreadyExists(t *testing.T) {
	fb := newFakeBackend()
	fb.alreadyExists["_grainfs"] = true
	fb.alreadyExists["default"] = true

	ctx := context.Background()
	if err := seedReservedBucket(ctx, fb, "_grainfs"); err != nil {
		t.Fatalf("second seed of _grainfs should succeed: %v", err)
	}
	if err := seedReservedBucket(ctx, fb, "default"); err != nil {
		t.Fatalf("second seed of default should succeed: %v", err)
	}
}

// wrappingBackend wraps an inner backend and exposes Unwrap() but does NOT
// itself implement reservedBucketSeeder, to verify chain traversal.
type wrappingBackend struct {
	storage.Backend
	inner storage.Backend
}

func (w *wrappingBackend) Unwrap() storage.Backend { return w.inner }
func (w *wrappingBackend) CreateBucket(ctx context.Context, bucket string) error {
	return w.inner.CreateBucket(ctx, bucket)
}

func TestFindReservedSeeder_TraversesUnwrapChain(t *testing.T) {
	fb := newFakeBackend()
	wrapped := &wrappingBackend{inner: fb}

	seeder, ok := findReservedSeeder(wrapped)
	if !ok {
		t.Fatal("expected to find reservedBucketSeeder via Unwrap chain")
	}
	if err := seeder.CreateBucketBypassReserved(context.Background(), "_grainfs"); err != nil {
		t.Fatalf("CreateBucketBypassReserved via unwrap: %v", err)
	}
	if len(fb.bypassCreated) != 1 || fb.bypassCreated[0] != "_grainfs" {
		t.Errorf("expected [_grainfs] bypass, got %v", fb.bypassCreated)
	}
}

func TestCreateDefaultBucketWithRetry_SeedsDefaultAndGrainfs(t *testing.T) {
	fb := newFakeBackend()
	ctx := context.Background()
	if err := CreateDefaultBucketWithRetry(ctx, fb, 0); err != nil {
		t.Fatalf("CreateDefaultBucketWithRetry: %v", err)
	}

	seeded := append(fb.bypassCreated, fb.created...)
	seededSet := map[string]bool{}
	for _, b := range seeded {
		seededSet[b] = true
	}
	for _, want := range []string{"default", "_grainfs"} {
		if !seededSet[want] {
			t.Errorf("bucket %q not seeded; seeded: %v", want, seeded)
		}
	}
}

func TestCreateDefaultBucketWithRetry_FallbackNoSeeder(t *testing.T) {
	// Backend with no bypass capability: seedReservedBucket falls back to
	// CreateBucket. Verify both buckets still get created.
	called := []string{}
	plain := &plainBackend{
		onCreate: func(bucket string) error {
			called = append(called, bucket)
			return nil
		},
	}
	ctx := context.Background()
	if err := CreateDefaultBucketWithRetry(ctx, plain, 0); err != nil {
		t.Fatalf("fallback path: %v", err)
	}
	has := func(s string) bool {
		for _, c := range called {
			if c == s {
				return true
			}
		}
		return false
	}
	for _, want := range []string{"default", "_grainfs"} {
		if !has(want) {
			t.Errorf("bucket %q not created via fallback; called: %v", want, called)
		}
	}
}

type plainBackend struct {
	storage.Backend
	onCreate func(string) error
	seen     map[string]bool
}

func (p *plainBackend) CreateBucket(_ context.Context, bucket string) error {
	if p.seen == nil {
		p.seen = map[string]bool{}
	}
	if p.seen[bucket] {
		return storage.ErrBucketAlreadyExists
	}
	p.seen[bucket] = true
	if p.onCreate != nil {
		return p.onCreate(bucket)
	}
	return nil
}

func TestCreateDefaultBucketWithRetry_ReturnsErrorAfterTimeout(t *testing.T) {
	persistentErr := errors.New("transient-fail")
	failing := &failingBackend{err: persistentErr}
	ctx := context.Background()
	err := CreateDefaultBucketWithRetry(ctx, failing, 0)
	if err == nil {
		t.Fatal("expected error when backend always fails")
	}
	if !errors.Is(err, persistentErr) {
		t.Errorf("expected wrapped persistentErr, got %v", err)
	}
}

type failingBackend struct {
	storage.Backend
	err error
}

func (f *failingBackend) CreateBucket(_ context.Context, _ string) error { return f.err }
