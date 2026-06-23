package lifecycle_test

import (
	"context"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/lifecycle"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// newBadger opens an in-process badger DB rooted at t.TempDir(). External-
// package twin of the internal newTestDB helper (store_test.go).
func newBadger(t *testing.T) *badger.DB {
	t.Helper()
	opts := badger.DefaultOptions(t.TempDir()).WithLogger(nil)
	db, err := badger.Open(opts)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return db
}

// --- backend double ---

type fakeMPUBackend struct {
	uploads []storage.MultipartUploadRecord
}

func (b *fakeMPUBackend) ListBuckets(ctx context.Context) ([]string, error) {
	_ = ctx
	return []string{"b"}, nil
}

func (b *fakeMPUBackend) ScanObjects(_ string) (<-chan scrubber.ObjectRecord, error) {
	ch := make(chan scrubber.ObjectRecord)
	close(ch)
	return ch, nil
}

func (b *fakeMPUBackend) ScanObjectsGrouped(_ string) (<-chan storage.ObjectKeyGroup, error) {
	ch := make(chan storage.ObjectKeyGroup)
	close(ch)
	return ch, nil
}

func (b *fakeMPUBackend) ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error) {
	ch := make(chan storage.MultipartUploadRecord, len(b.uploads))
	for _, u := range b.uploads {
		if u.Bucket == bucket {
			ch <- u
		}
	}
	close(ch)
	return ch, nil
}

// --- deleter double ---

type fakeMPUDeleter struct {
	aborted   []string
	partCount int // returned by MultipartUploadPartCount; 0 means "unknown"
	waitN     []int
}

func (d *fakeMPUDeleter) DeleteObject(_ context.Context, _, _ string) error { return nil }
func (d *fakeMPUDeleter) DeleteObjectVersion(_, _, _ string) error          { return nil }
func (d *fakeMPUDeleter) ListObjectVersions(_ context.Context, _, _ string, _ int) ([]*storage.ObjectVersion, error) {
	return nil, nil
}

func (d *fakeMPUDeleter) AbortMultipartUpload(_ context.Context, _, _, uploadID string) error {
	d.aborted = append(d.aborted, uploadID)
	return nil
}

func (d *fakeMPUDeleter) MultipartUploadPartCount(_, _, _ string) (int, error) {
	return d.partCount, nil
}

func (d *fakeMPUDeleter) AbortedUploadIDs() []string { return d.aborted }

// --- tests ---

func TestMPUWorker_AbortsExpiredUploads(t *testing.T) {
	now := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
	backend := &fakeMPUBackend{uploads: []storage.MultipartUploadRecord{
		{Bucket: "b", Key: "k1", UploadID: "u1", InitiatedAt: now.Add(-4 * 24 * time.Hour).Unix()},
		{Bucket: "b", Key: "k2", UploadID: "u2", InitiatedAt: now.Add(-1 * time.Hour).Unix()},
	}}
	deleter := &fakeMPUDeleter{}
	store := lifecycle.NewStore(newBadger(t))
	require.NoError(t, store.PutRaw("b", []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)))

	limiter := rate.NewLimiter(100, 10)
	w := lifecycle.NewMPUWorker(store, backend, deleter, time.Minute, limiter, lifecycle.WithMPUNowForTest(func() time.Time { return now }))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.RunCycleForTest(ctx)

	require.Equal(t, []string{"u1"}, deleter.AbortedUploadIDs())
	require.Equal(t, int64(1), w.AbortedTotal())
}

func TestMPUWorker_RespectsPrefixFilter(t *testing.T) {
	now := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
	backend := &fakeMPUBackend{uploads: []storage.MultipartUploadRecord{
		{Bucket: "b", Key: "tmp/u1", UploadID: "u1", InitiatedAt: now.Add(-4 * 24 * time.Hour).Unix()},
		{Bucket: "b", Key: "other/u2", UploadID: "u2", InitiatedAt: now.Add(-4 * 24 * time.Hour).Unix()},
	}}
	deleter := &fakeMPUDeleter{}
	store := lifecycle.NewStore(newBadger(t))
	require.NoError(t, store.PutRaw("b", []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Filter><Prefix>tmp/</Prefix></Filter><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)))

	limiter := rate.NewLimiter(100, 10)
	w := lifecycle.NewMPUWorker(store, backend, deleter, time.Minute, limiter, lifecycle.WithMPUNowForTest(func() time.Time { return now }))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.RunCycleForTest(ctx)

	require.Equal(t, []string{"u1"}, deleter.AbortedUploadIDs())
}

// TestMPUWorker_PartCountAboveBurst confirms that an upload reporting a part
// count above the limiter's burst still gets aborted — i.e. the worker caps
// the WaitN weight at burst rather than failing the wait outright.
func TestMPUWorker_PartCountAboveBurst(t *testing.T) {
	now := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
	backend := &fakeMPUBackend{uploads: []storage.MultipartUploadRecord{
		{Bucket: "b", Key: "k1", UploadID: "u1", InitiatedAt: now.Add(-4 * 24 * time.Hour).Unix()},
	}}
	deleter := &fakeMPUDeleter{partCount: 1000} // far above burst
	store := lifecycle.NewStore(newBadger(t))
	require.NoError(t, store.PutRaw("b", []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)))

	limiter := rate.NewLimiter(rate.Inf, 10)
	w := lifecycle.NewMPUWorker(store, backend, deleter, time.Minute, limiter, lifecycle.WithMPUNowForTest(func() time.Time { return now }))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.RunCycleForTest(ctx)

	require.Equal(t, []string{"u1"}, deleter.AbortedUploadIDs())
}

// --- F5 repoint: manifest-blob model (scan + idempotent abort share a store) ---

// manifestBlobStore models the off-FSM .qmeta_mpu replicas: ScanLocalMultipartUploads
// emits the manifests present, and an Abort idempotently deletes one (a re-abort
// or a second node's abort of an already-gone manifest is a harmless no-op).
type manifestBlobStore struct {
	manifests map[string]storage.MultipartUploadRecord // uploadID → record
}

func (s *manifestBlobStore) ListBuckets(context.Context) ([]string, error) {
	return []string{"b"}, nil
}
func (s *manifestBlobStore) ScanObjects(string) (<-chan scrubber.ObjectRecord, error) {
	ch := make(chan scrubber.ObjectRecord)
	close(ch)
	return ch, nil
}
func (s *manifestBlobStore) ScanObjectsGrouped(string) (<-chan storage.ObjectKeyGroup, error) {
	ch := make(chan storage.ObjectKeyGroup)
	close(ch)
	return ch, nil
}
func (s *manifestBlobStore) ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error) {
	ch := make(chan storage.MultipartUploadRecord, len(s.manifests))
	for _, u := range s.manifests {
		if u.Bucket == bucket {
			ch <- u
		}
	}
	close(ch)
	return ch, nil
}

// manifestBlobDeleter idempotently deletes from the shared manifest store, counting
// only the deletes that actually removed a manifest (the real deleteManifestBlob
// no-ops on an absent blob).
type manifestBlobDeleter struct {
	store         *manifestBlobStore
	effectiveAbts []string // aborts that actually removed a manifest
	totalAbts     int      // every abort call, incl. idempotent no-ops
}

func (d *manifestBlobDeleter) DeleteObject(context.Context, string, string) error { return nil }
func (d *manifestBlobDeleter) DeleteObjectVersion(string, string, string) error   { return nil }
func (d *manifestBlobDeleter) ListObjectVersions(context.Context, string, string, int) ([]*storage.ObjectVersion, error) {
	return nil, nil
}
func (d *manifestBlobDeleter) MultipartUploadPartCount(string, string, string) (int, error) {
	return 0, nil
}
func (d *manifestBlobDeleter) AbortMultipartUpload(_ context.Context, _, _, uploadID string) error {
	d.totalAbts++
	if _, ok := d.store.manifests[uploadID]; ok {
		delete(d.store.manifests, uploadID)
		d.effectiveAbts = append(d.effectiveAbts, uploadID)
	}
	return nil // idempotent: an already-gone manifest is a no-op (no error)
}

// TestMPUWorker_F5RepointIdempotent proves the node-local manifest-blob repoint:
// an over-age in-flight upload (manifest present) is found and aborted; a second
// worker run (or a second node's worker) re-scans, sees the manifest gone, and
// performs no double-effect — and even a duplicate abort of an already-gone
// manifest returns no error (the idempotent deleteManifestBlob model).
func TestMPUWorker_F5RepointIdempotent(t *testing.T) {
	now := time.Date(2026, 5, 19, 0, 0, 0, 0, time.UTC)
	overAge := now.Add(-4 * 24 * time.Hour).Unix()
	mstore := &manifestBlobStore{manifests: map[string]storage.MultipartUploadRecord{
		"u1": {Bucket: "b", Key: "k1", UploadID: "u1", InitiatedAt: overAge},
	}}
	deleter := &manifestBlobDeleter{store: mstore}
	store := lifecycle.NewStore(newBadger(t))
	require.NoError(t, store.PutRaw("b", []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><AbortIncompleteMultipartUpload><DaysAfterInitiation>3</DaysAfterInitiation></AbortIncompleteMultipartUpload></Rule></LifecycleConfiguration>`)))

	limiter := rate.NewLimiter(rate.Inf, 10)
	w := lifecycle.NewMPUWorker(store, mstore, deleter, time.Minute, limiter, lifecycle.WithMPUNowForTest(func() time.Time { return now }))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// First run: the over-age upload is found and aborted.
	w.RunCycleForTest(ctx)
	require.Equal(t, []string{"u1"}, deleter.effectiveAbts, "over-age upload must be aborted after the repoint")
	require.Equal(t, int64(1), w.AbortedTotal())

	// Second run (idempotent): the manifest is gone, so no further abort fires.
	w.RunCycleForTest(ctx)
	require.Equal(t, []string{"u1"}, deleter.effectiveAbts, "a second worker run must not re-abort a gone manifest")

	// A second node's abort of the already-gone manifest is a harmless no-op.
	require.NoError(t, deleter.AbortMultipartUpload(ctx, "b", "k1", "u1"),
		"duplicate abort of a gone manifest must not error")
	require.Equal(t, []string{"u1"}, deleter.effectiveAbts, "duplicate abort must have no double-effect")
}
