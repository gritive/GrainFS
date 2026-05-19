package lifecycle

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// --- mock backend ---

// mockBackend implements lifecycle.Scrubbable for unit tests. When `groups`
// is set for a bucket, ScanObjectsGrouped returns those groups directly;
// otherwise it synthesises one group per entry in `objects` (one synthetic
// version each, marked IsLatest=true, propagating IsDeleteMarker). The
// scrubber-style `objects` field stays for ScanObjects compatibility and
// for tests that pre-date Task 9.
type mockBackend struct {
	buckets []string
	objects map[string][]scrubber.ObjectRecord
	groups  map[string][]storage.ObjectKeyGroup
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

func (m *mockBackend) ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error) {
	var groups []storage.ObjectKeyGroup
	if g, ok := m.groups[bucket]; ok {
		groups = append([]storage.ObjectKeyGroup(nil), g...)
	} else {
		for _, r := range m.objects[bucket] {
			groups = append(groups, storage.ObjectKeyGroup{
				Bucket: r.Bucket,
				Key:    r.Key,
				Versions: []storage.ObjectVersionRecord{{
					IsLatest:       true,
					IsDeleteMarker: r.IsDeleteMarker,
					LastModified:   r.LastModified,
				}},
			})
		}
	}
	ch := make(chan storage.ObjectKeyGroup, len(groups))
	for _, g := range groups {
		ch <- g
	}
	close(ch)
	return ch, nil
}

func (m *mockBackend) ScanLocalMultipartUploads(bucket string) (<-chan storage.MultipartUploadRecord, error) {
	_ = bucket
	ch := make(chan storage.MultipartUploadRecord)
	close(ch)
	return ch, nil
}

// --- mock deleter ---

type mockDeleter struct {
	mu                      sync.Mutex
	deleted                 []string                            // "bucket/key"
	deletedVersions         []string                            // "bucket/key/versionID"
	versions                map[string][]*storage.ObjectVersion // "bucket/key" → versions
	listObjectVersionsCalls int
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
	m.listObjectVersionsCalls++
	return m.versions[bucket+"/"+prefix], nil
}

func (m *mockDeleter) AbortMultipartUpload(_ context.Context, _, _, _ string) error {
	return nil
}

func (m *mockDeleter) MultipartUploadPartCount(_, _, _ string) (int, error) {
	return 0, nil
}

func newWorker(store *Store, backend *mockBackend, deleter *mockDeleter) *Worker {
	return &Worker{
		store:   store,
		backend: backend,
		deleter: deleter,
		limiter: rate.NewLimiter(rate.Inf, 0),
		now:     time.Now,
	}
}

var _ ObjectDeleter = (*storage.Operations)(nil)

func TestWorker_UsesStorageOperationsForMutations(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)
	require.NoError(t, store.put("bucket", &LifecycleConfiguration{
		Rules: []Rule{{
			ID:         "expire-1",
			Status:     "Enabled",
			Expiration: &Expiration{Days: 1},
		}},
	}))

	oldTime := time.Now().Add(-2 * 24 * time.Hour).Unix()
	scanner := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{Bucket: "bucket", Key: "obj", LastModified: oldTime}},
		},
	}
	mutations := &operationsLifecycleBackend{}
	w := &Worker{
		store:   store,
		backend: scanner,
		deleter: storage.NewOperations(mutations),
		limiter: rate.NewLimiter(rate.Inf, 0),
	}

	w.runCycle(context.Background())

	assert.Equal(t, []string{"bucket/obj"}, mutations.deleted)
}

type operationsLifecycleBackend struct {
	storage.Backend
	deleted         []string
	deletedVersions []string
	versions        []*storage.ObjectVersion
}

func (b *operationsLifecycleBackend) DeleteObject(_ context.Context, bucket, key string) error {
	b.deleted = append(b.deleted, bucket+"/"+key)
	return nil
}

func (b *operationsLifecycleBackend) DeleteObjectVersion(bucket, key, versionID string) error {
	b.deletedVersions = append(b.deletedVersions, bucket+"/"+key+"/"+versionID)
	return nil
}

func (b *operationsLifecycleBackend) ListObjectVersions(bucket, prefix string, _ int) ([]*storage.ObjectVersion, error) {
	return b.versions, nil
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
	require.NoError(t, store.put("bucket", cfg))

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

func TestWorker_ExpirationDaysSkipsDeleteMarkers(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)
	now := time.Unix(1700000000, 0).UTC()

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{
			ID:         "expire-1",
			Status:     "Enabled",
			Expiration: &Expiration{Days: 1},
		}},
	}
	require.NoError(t, store.put("bucket", cfg))

	backend := &mockBackend{
		buckets: []string{"bucket"},
		objects: map[string][]scrubber.ObjectRecord{
			"bucket": {{
				Bucket:         "bucket",
				Key:            "marker",
				LastModified:   now.Add(-48 * time.Hour).Unix(),
				IsDeleteMarker: true,
			}},
		},
	}
	deleter := &mockDeleter{}

	w := newWorker(store, backend, deleter)
	w.now = func() time.Time { return now }
	w.runCycle(context.Background())

	assert.Empty(t, deleter.deleted)
	assert.Equal(t, int64(1), w.Stats().ObjectsChecked)
	assert.Zero(t, w.Stats().Expired)
}

// TestWorker_SkipsRecentObject verifies that an object newer than Expiration.Days
// is NOT deleted.
func TestWorker_SkipsRecentObject(t *testing.T) {
	db := newTestDB(t)
	store := NewStore(db)

	cfg := &LifecycleConfiguration{
		Rules: []Rule{{ID: "r", Status: "Enabled", Expiration: &Expiration{Days: 30}}},
	}
	require.NoError(t, store.put("bucket", cfg))

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
	require.NoError(t, store.put("bucket", cfg))

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
	require.NoError(t, store.put("bucket", cfg))

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
// NewerNoncurrentVersions versions works. Drives the post-Task-9 worker which
// walks g.Versions directly (no per-object ListObjectVersions call).
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
	require.NoError(t, store.put("bucket", cfg))

	oldTime := time.Now().Add(-100 * 24 * time.Hour).Unix()
	backend := &mockBackend{
		buckets: []string{"bucket"},
		groups: map[string][]storage.ObjectKeyGroup{
			"bucket": {{
				Bucket: "bucket",
				Key:    "obj",
				Versions: []storage.ObjectVersionRecord{
					{VersionID: "v4", IsLatest: true, LastModified: oldTime},
					{VersionID: "v3", LastModified: oldTime}, // keep (noncurrentIdx 0 < 2)
					{VersionID: "v2", LastModified: oldTime}, // keep (noncurrentIdx 1 < 2)
					{VersionID: "v1", LastModified: oldTime}, // delete (noncurrentIdx 2 >= 2)
				},
			}},
		},
	}
	deleter := &mockDeleter{}

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
	require.NoError(t, store.put("bucket", cfg))

	recentTime := time.Now().Add(-3 * 24 * time.Hour).Unix()
	oldTime := time.Now().Add(-10 * 24 * time.Hour).Unix()

	backend := &mockBackend{
		buckets: []string{"bucket"},
		groups: map[string][]storage.ObjectKeyGroup{
			"bucket": {{
				Bucket: "bucket",
				Key:    "obj",
				Versions: []storage.ObjectVersionRecord{
					{VersionID: "v2", IsLatest: true, LastModified: recentTime},
					{VersionID: "v1", LastModified: oldTime},
				},
			}},
		},
	}
	deleter := &mockDeleter{}

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
	require.NoError(t, store.put("bucket", cfg))

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

// --- regression guard: post-Task-9 worker drives ScanObjectsGrouped and
// must NOT call ListObjectVersions on the per-object path. ---

type countingBackend struct {
	buckets               []string
	keys                  int
	scanObjectsGroupedHit atomic.Int64
	scanLocalMPUHit       atomic.Int64
	listBucketsHit        atomic.Int64
}

func newCountingBackend(_ *testing.T, n int) *countingBackend {
	return &countingBackend{buckets: []string{"b"}, keys: n}
}

func (c *countingBackend) ListBuckets(_ context.Context) ([]string, error) {
	c.listBucketsHit.Add(1)
	return c.buckets, nil
}

func (c *countingBackend) ScanObjects(_ string) (<-chan scrubber.ObjectRecord, error) {
	ch := make(chan scrubber.ObjectRecord)
	close(ch)
	return ch, nil
}

func (c *countingBackend) ScanObjectsGrouped(bucket string) (<-chan storage.ObjectKeyGroup, error) {
	c.scanObjectsGroupedHit.Add(1)
	old := time.Now().Add(-30 * 24 * time.Hour).Unix()
	ch := make(chan storage.ObjectKeyGroup, c.keys)
	for i := 0; i < c.keys; i++ {
		ch <- storage.ObjectKeyGroup{
			Bucket: bucket,
			Key:    "k" + itoa(i),
			Versions: []storage.ObjectVersionRecord{{
				VersionID:    "v",
				IsLatest:     true,
				LastModified: old,
			}},
		}
	}
	close(ch)
	return ch, nil
}

func (c *countingBackend) ScanLocalMultipartUploads(_ string) (<-chan storage.MultipartUploadRecord, error) {
	c.scanLocalMPUHit.Add(1)
	ch := make(chan storage.MultipartUploadRecord)
	close(ch)
	return ch, nil
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	const digits = "0123456789"
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = digits[i%10]
		i /= 10
	}
	return string(buf[pos:])
}

// TestWorker_NoNListVersionsCalls asserts that one cycle over N keys does not
// trigger any ListObjectVersions calls on the deleter — the post-Task-9 worker
// must derive everything from ScanObjectsGrouped. Also asserts the grouped
// scan was actually invoked, so the test cannot pass by silently doing nothing.
func TestWorker_NoNListVersionsCalls(t *testing.T) {
	const N = 100
	db := newTestDB(t)
	store := NewStore(db)
	raw := []byte(`<LifecycleConfiguration><Rule><ID>r</ID><Status>Enabled</Status><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>`)
	require.NoError(t, store.PutRaw("b", raw))

	backend := newCountingBackend(t, N)
	deleter := &mockDeleter{}

	w := NewWorker(store, backend, deleter, time.Minute)
	w.limiter = rate.NewLimiter(rate.Inf, 0) // disable rate-limit pacing in tests
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.RunCycleForTest(ctx)

	deleter.mu.Lock()
	calls := deleter.listObjectVersionsCalls
	deleter.mu.Unlock()
	assert.Zero(t, calls, "new worker must not call ListObjectVersions on the per-object path")
	assert.GreaterOrEqual(t, backend.scanObjectsGroupedHit.Load(), int64(1),
		"worker must drive ScanObjectsGrouped — non-vacuous guard")
}
