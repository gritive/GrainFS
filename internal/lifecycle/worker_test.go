package lifecycle

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// --- mock backend ---

type mockBackend struct {
	buckets []string
	objects map[string][]scrubber.ObjectRecord
}

func (m *mockBackend) ListBuckets(ctx context.Context) ([]string, error) {
	_ = ctx
	return m.buckets, nil
}

func (m *mockBackend) ScanObjects(bucket string) (<-chan scrubber.ObjectRecord, error) {
	recs := append([]scrubber.ObjectRecord(nil), m.objects[bucket]...)
	ch := make(chan scrubber.ObjectRecord, len(recs))
	for _, r := range recs {
		ch <- r
	}
	close(ch)
	return ch, nil
}

// --- mock deleter ---

type mockDeleter struct {
	mu              sync.Mutex
	deleted         []string                            // "bucket/key"
	deletedVersions []string                            // "bucket/key/versionID"
	versions        map[string][]*storage.ObjectVersion // "bucket/key" → versions
}

func (m *mockDeleter) DeleteObject(ctx context.Context, bucket, key string) error {
	_ = ctx
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deleted = append(m.deleted, bucket+"/"+key)
	return nil
}

func (m *mockDeleter) DeleteObjectVersion(bucket, key, versionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedVersions = append(m.deletedVersions, bucket+"/"+key+"/"+versionID)
	return nil
}

func (m *mockDeleter) ListObjectVersions(bucket, prefix string, _ int) ([]*storage.ObjectVersion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.versions[bucket+"/"+prefix], nil
}

func newWorker(store *Store, backend *mockBackend, deleter *mockDeleter) *Worker {
	return &Worker{
		store:   store,
		backend: backend,
		deleter: deleter,
		limiter: rate.NewLimiter(rate.Inf, 0),
	}
}

// TestWorker_ExpiresOldObject verifies that an object older than Expiration.Days
// triggers DeleteObject.
func TestWorker_ExpiresOldObject(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{
			ID:         "expire-30",
			Status:     "Enabled",
			Expiration: &Expiration{Days: 30},
		}},
	}
	require.NoError(t, store.Put("bucket", cfg))

	oldTime := time.Now().Add(-31 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{Bucket: "bucket", Key: "old.log", DataShards: 4, LastModified: oldTime}},
		},
	}
	deleter := &mockDeleter{}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	assert.Equal(t, []string{"bucket/old.log"}, deleter.deleted)
}

// TestWorker_SkipsRecentObject verifies that an object newer than Expiration.Days
// is NOT deleted.
func TestWorker_SkipsRecentObject(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{ID: "r", Status: "Enabled", Expiration: &Expiration{Days: 30}}},
	}
	require.NoError(t, store.Put("bucket", cfg))

	newTime := time.Now().Add(-10 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{Bucket: "bucket", Key: "recent.log", DataShards: 4, LastModified: newTime}},
		},
	}
	deleter := &mockDeleter{}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	assert.Empty(t, deleter.deleted)
}

// TestWorker_SkipsDisabledRule verifies that a disabled rule has no effect.
func TestWorker_SkipsDisabledRule(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{ID: "r", Status: "Disabled", Expiration: &Expiration{Days: 1}}},
	}
	require.NoError(t, store.Put("bucket", cfg))

	oldTime := time.Now().Add(-100 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{Bucket: "bucket", Key: "obj", DataShards: 4, LastModified: oldTime}},
		},
	}
	deleter := &mockDeleter{}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	assert.Empty(t, deleter.deleted)
}

// TestWorker_PrefixFilter verifies that only objects matching the prefix are expired.
func TestWorker_PrefixFilter(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{
			ID:         "expire-logs",
			Status:     "Enabled",
			Filter:     &Filter{Prefix: "logs/"},
			Expiration: &Expiration{Days: 1},
		}},
	}
	require.NoError(t, store.Put("bucket", cfg))

	oldTime := time.Now().Add(-2 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {
				{Bucket: "bucket", Key: "logs/app.log", DataShards: 4, LastModified: oldTime},
				{Bucket: "bucket", Key: "data/keep.bin", DataShards: 4, LastModified: oldTime},
			},
		},
	}
	deleter := &mockDeleter{}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	assert.Equal(t, []string{"bucket/logs/app.log"}, deleter.deleted)
}

// TestWorker_NoBucketConfig verifies that buckets without lifecycle config are skipped.
func TestWorker_NoBucketConfig(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)
	// no config stored

	oldTime := time.Now().Add(-100 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{Bucket: "bucket", Key: "obj", DataShards: 4, LastModified: oldTime}},
		},
	}
	deleter := &mockDeleter{}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	assert.Empty(t, deleter.deleted)
}

// TestWorker_NoncurrentVersionExpiration_ByCount verifies that keeping only
// NewerNoncurrentVersions versions works.
func TestWorker_NoncurrentVersionExpiration_ByCount(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{
			ID:     "keep-2",
			Status: "Enabled",
			NoncurrentVersionExpiration: &NoncurrentVersionExpiration{
				NewerNoncurrentVersions: 2,
			},
		}},
	}
	require.NoError(t, store.Put("bucket", cfg))

	oldTime := time.Now().Add(-100 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{Bucket: "bucket", Key: "obj", DataShards: 4, LastModified: oldTime}},
		},
	}
	deleter := &mockDeleter{
		versions: map[string][]*storage.ObjectVersion{
			"bucket/obj": {
				{Key: "obj", VersionID: "v4", IsLatest: true, LastModified: oldTime},
				{Key: "obj", VersionID: "v3", IsLatest: false, LastModified: oldTime}, // keep (index 1 < 2+1=3)
				{Key: "obj", VersionID: "v2", IsLatest: false, LastModified: oldTime}, // keep (index 2 < 3)
				{Key: "obj", VersionID: "v1", IsLatest: false, LastModified: oldTime}, // delete (index 3 >= 3)
			},
		},
	}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	assert.Empty(t, deleter.deleted, "current version must not be deleted")
	assert.Equal(t, []string{"bucket/obj/v1"}, deleter.deletedVersions)
}

// TestWorker_NoncurrentVersionExpiration_ByAge verifies that NoncurrentDays
// deletes versions older than the threshold.
func TestWorker_NoncurrentVersionExpiration_ByAge(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{
			ID:                          "prune-old",
			Status:                      "Enabled",
			NoncurrentVersionExpiration: &NoncurrentVersionExpiration{NoncurrentDays: 7},
		}},
	}
	require.NoError(t, store.Put("bucket", cfg))

	recentTime := time.Now().Add(-3 * 24 * time.Hour).Unix()
	oldTime := time.Now().Add(-10 * 24 * time.Hour).Unix()

	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{Bucket: "bucket", Key: "obj", DataShards: 4, LastModified: recentTime}},
		},
	}
	deleter := &mockDeleter{
		versions: map[string][]*storage.ObjectVersion{
			"bucket/obj": {
				{Key: "obj", VersionID: "v2", IsLatest: true, LastModified: recentTime},
				{Key: "obj", VersionID: "v1", IsLatest: false, LastModified: oldTime},
			},
		},
	}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	assert.Equal(t, []string{"bucket/obj/v1"}, deleter.deletedVersions)
}

// TestWorker_Stats tracks ObjectsChecked and Expired counts.
func TestWorker_Stats(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{ID: "r", Status: "Enabled", Expiration: &Expiration{Days: 1}}},
	}
	require.NoError(t, store.Put("bucket", cfg))

	oldTime := time.Now().Add(-2 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {
				{Bucket: "bucket", Key: "a", DataShards: 4, LastModified: oldTime},
				{Bucket: "bucket", Key: "b", DataShards: 4, LastModified: oldTime},
			},
		},
	}
	deleter := &mockDeleter{}

	w := newWorker(store, backend, deleter)
	w.runCycle(context.Background())

	stats := w.Stats()
	assert.Equal(t, int64(2), stats.ObjectsChecked)
	assert.Equal(t, int64(2), stats.Expired)
}
