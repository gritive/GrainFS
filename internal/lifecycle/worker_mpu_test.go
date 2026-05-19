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
func (d *fakeMPUDeleter) ListObjectVersions(_, _ string, _ int) ([]*storage.ObjectVersion, error) {
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
