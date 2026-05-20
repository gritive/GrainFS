package cluster

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/storage/eccodec"
)

// Compile-time proof that DistributedBackend satisfies scrubber.Scrubbable.
var _ scrubber.Scrubbable = (*DistributedBackend)(nil)
var _ scrubber.ShardIntegrityReader = (*DistributedBackend)(nil)

// writeVersionedObjectMeta seeds the FSM with a versioned object metadata
// entry plus its `lat:` pointer, mirroring what applyPutObjectMeta writes.
// Used by scan/exists tests that don't need to go through the EC put path.
func writeVersionedObjectMeta(t *testing.T, b *DistributedBackend, bucket, key, versionID, etag string, size int64) {
	t.Helper()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
	})
	require.NoError(t, err)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKey(bucket, key), meta); err != nil {
			return err
		}
		if err := txn.Set(objectMetaKeyV(bucket, key, versionID), meta); err != nil {
			return err
		}
		return txn.Set(latestKey(bucket, key), []byte(versionID))
	}))
}

func drainObjectRecords(t *testing.T, ch <-chan scrubber.ObjectRecord) []scrubber.ObjectRecord {
	t.Helper()
	var out []scrubber.ObjectRecord
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, rec)
		case <-ctx.Done():
			t.Fatalf("channel did not close within timeout")
			return out
		}
	}
}

// enableECForTest wires a 2+1 EC config onto the backend and makes ECActive
// return true by padding allNodes to k+m entries. Avoids spinning up a
// multi-node cluster just to exercise ScanObjects routing.
func enableECForTest(t *testing.T, b *DistributedBackend, k, m int) {
	t.Helper()
	b.SetECConfig(ECConfig{DataShards: k, ParityShards: m})
	nodes := make([]string, 0, k+m)
	for i := 0; i < k+m; i++ {
		nodes = append(nodes, fmt.Sprintf("node-%d", i))
	}
	b.allNodes = nodes
	require.True(t, b.ECActive(), "ECActive must be true after enable")
}

func TestScanObjects_EmptyBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	enableECForTest(t, b, 2, 1)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	ch, err := b.ScanObjects("bkt")
	require.NoError(t, err)
	recs := drainObjectRecords(t, ch)
	assert.Empty(t, recs)
}

func TestScanObjects_MissingBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	enableECForTest(t, b, 2, 1)

	_, err := b.ScanObjects("no-such-bucket")
	assert.Error(t, err)
}

func TestScanObjects_SingleObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	enableECForTest(t, b, 2, 1)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	writeVersionedObjectMeta(t, b, "bkt", "hello.txt", "01HZXYZABC", "etag-hello", 11)

	ch, err := b.ScanObjects("bkt")
	require.NoError(t, err)
	recs := drainObjectRecords(t, ch)

	require.Len(t, recs, 1)
	assert.Equal(t, "bkt", recs[0].Bucket)
	assert.Equal(t, "hello.txt", recs[0].Key)
	assert.Equal(t, "01HZXYZABC", recs[0].VersionID)
	assert.Equal(t, 2, recs[0].DataShards)
	assert.Equal(t, 1, recs[0].ParityShards)
	assert.Equal(t, "etag-hello", recs[0].ETag)
}

func TestScanObjects_MultipleObjectsIncludingSlashKey(t *testing.T) {
	b := newTestDistributedBackend(t)
	enableECForTest(t, b, 2, 1)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	// Key containing '/' — lat: iteration must treat this as a single key.
	writeVersionedObjectMeta(t, b, "bkt", "folder/nested/file.bin", "01A", "etag-a", 100)
	writeVersionedObjectMeta(t, b, "bkt", "top.txt", "01B", "etag-b", 7)

	ch, err := b.ScanObjects("bkt")
	require.NoError(t, err)
	recs := drainObjectRecords(t, ch)

	require.Len(t, recs, 2)
	keys := map[string]bool{}
	for _, r := range recs {
		keys[r.Key] = true
	}
	assert.True(t, keys["folder/nested/file.bin"], "slash-containing key must appear intact")
	assert.True(t, keys["top.txt"])
}

func TestScanObjects_SkipsTombstones(t *testing.T) {
	b := newTestDistributedBackend(t)
	enableECForTest(t, b, 2, 1)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	writeVersionedObjectMeta(t, b, "bkt", "alive", "01A", "etag-alive", 5)
	writeVersionedObjectMeta(t, b, "bkt", "dead", "01D", deleteMarkerETag, 0)

	ch, err := b.ScanObjects("bkt")
	require.NoError(t, err)
	recs := drainObjectRecords(t, ch)

	require.Len(t, recs, 1, "tombstone must be filtered")
	assert.Equal(t, "alive", recs[0].Key)
}

func TestObjectExists(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))
	writeVersionedObjectMeta(t, b, "bkt", "present", "01A", "etag", 10)
	writeVersionedObjectMeta(t, b, "bkt", "tomb", "01B", deleteMarkerETag, 0)

	ok, err := b.ObjectExists("bkt", "present")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = b.ObjectExists("bkt", "missing")
	require.NoError(t, err)
	assert.False(t, ok)

	ok, err = b.ObjectExists("bkt", "tomb")
	require.NoError(t, err)
	assert.False(t, ok, "tombstones must not be reported as existing")

	ok, err = b.ObjectExists("no-such-bucket", "k")
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestShardPaths(t *testing.T) {
	b := newTestDistributedBackend(t)
	paths := b.ShardPaths("bkt", "objects/big.bin", "01HZXYZ", 6)

	require.Len(t, paths, 6)
	for i, p := range paths {
		assert.Equal(t,
			filepath.Join(b.root, "shards", "bkt", "objects/big.bin", "01HZXYZ", fmt.Sprintf("shard_%d", i)),
			p,
		)
	}
}

func TestShardPaths_MatchesShardServiceLayout(t *testing.T) {
	// ShardPaths must return the same path that putObjectEC writes to via
	// ShardService.WriteLocalShard. putObjectEC composes shardKey = key +
	// "/" + versionID before calling ShardService; ShardPaths mirrors that
	// layout so scrubber reads find what the EC put flow wrote.
	b := newTestDistributedBackend(t)

	svc := NewShardService(b.root, nil)
	require.NoError(t, svc.WriteLocalShard("bkt", "key/01VID", 3, []byte("payload")))

	paths := b.ShardPaths("bkt", "key", "01VID", 4)
	data, err := os.ReadFile(paths[3])
	require.NoError(t, err, "path from ShardPaths must find the shard on disk")
	decoded, err := eccodec.DecodeShard(data)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(decoded))
}

func TestShardPaths_UsesSharedShardServiceRoot(t *testing.T) {
	b := newTestDistributedBackend(t)
	shardRoot := t.TempDir()
	svc := NewShardService(shardRoot, nil)
	b.SetShardService(svc, []string{"test-node"})
	require.NoError(t, svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("payload")))

	paths := b.ShardPaths("bkt", "key", "01VID", 1)
	data, err := os.ReadFile(paths[0])
	require.NoError(t, err, "group backends must read shards from the shared ShardService root")
	decoded, err := eccodec.DecodeShard(data)
	require.NoError(t, err)
	assert.Equal(t, "payload", string(decoded))
}

func TestWriteShard_ReadShard_RoundTrip(t *testing.T) {
	b := newTestDistributedBackend(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "shard_0")

	payload := []byte("ec-shard-payload-0x42")
	require.NoError(t, b.WriteShard("bkt", "k", path, payload))

	got, err := b.ReadShard("bkt", "k", path)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(payload, got), "round-trip mismatch")
}

func TestReadShardIntegrity_EncodedVerified(t *testing.T) {
	b := newTestDistributedBackend(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")

	payload := []byte("encoded-payload")
	require.NoError(t, b.WriteShard("bkt", "k", path, payload))

	got, err := b.ReadShardIntegrity("bkt", "k", path)
	require.NoError(t, err)
	assert.Equal(t, scrubber.ShardIntegrityVerified, got.Status)
	assert.Equal(t, payload, got.Payload)
}

func TestReadShardIntegrity_EncryptedShardServiceShardVerified(t *testing.T) {
	b := newTestDistributedBackend(t)
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{7}, 32))
	require.NoError(t, err)
	svc := NewShardService(t.TempDir(), nil, WithEncryptor(enc))
	b.SetShardService(svc, []string{"test-node"})
	require.NoError(t, svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("payload")))

	path := b.ShardPaths("bkt", "key", "01VID", 1)[0]
	got, err := b.ReadShardIntegrity("bkt", "key", path)
	require.NoError(t, err)
	assert.Equal(t, scrubber.ShardIntegrityVerified, got.Status)
	assert.Equal(t, "payload", string(got.Payload))
}

func TestReadShardIntegrity_SharedPackShardVerified(t *testing.T) {
	b := newTestDistributedBackend(t)
	enc, err := encrypt.NewEncryptor(bytes.Repeat([]byte{8}, 32))
	require.NoError(t, err)
	svc := NewShardService(t.TempDir(), nil, WithEncryptor(enc), WithShardPackThreshold(1024))
	b.SetShardService(svc, []string{"test-node"})
	require.NoError(t, svc.WriteLocalShard("bkt", "key/01VID", 0, []byte("packed-payload")))

	path := b.ShardPaths("bkt", "key", "01VID", 1)[0]
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)

	got, err := b.ReadShardIntegrity("bkt", "key", path)
	require.NoError(t, err)
	assert.Equal(t, scrubber.ShardIntegrityVerified, got.Status)
	assert.Equal(t, "packed-payload", string(got.Payload))
}

func TestReadShardIntegrity_LegacyRawUnverifiedButReadShardCompatible(t *testing.T) {
	b := newTestDistributedBackend(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")
	payload := []byte("legacy-raw-payload")
	require.NoError(t, os.WriteFile(path, payload, 0o600))

	got, err := b.ReadShardIntegrity("bkt", "k", path)
	require.NoError(t, err)
	assert.Equal(t, scrubber.ShardIntegrityUnverifiedLegacy, got.Status)
	assert.Equal(t, payload, got.Payload)

	compat, err := b.ReadShard("bkt", "k", path)
	require.NoError(t, err)
	assert.Equal(t, payload, compat)
}

func TestWriteShard_AtomicOverwrite(t *testing.T) {
	b := newTestDistributedBackend(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "shard_0")

	require.NoError(t, b.WriteShard("bkt", "k", path, []byte("v1")))
	require.NoError(t, b.WriteShard("bkt", "k", path, []byte("v2")))

	got, err := b.ReadShard("bkt", "k", path)
	require.NoError(t, err)
	assert.Equal(t, "v2", string(got))

	// No leftover .tmp files after success.
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, e := range entries {
		assert.False(t, strings.HasSuffix(e.Name(), ".tmp"), "leftover tmp: %s", e.Name())
	}
}

func TestReadShard_MissingFile(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.ReadShard("bkt", "k", filepath.Join(t.TempDir(), "nope"))
	require.Error(t, err)
}

// --- Task 6: ScanObjectsGrouped / ScanLocalMultipartUploads ---

// writeVersionedObjectMetaTagged seeds a versioned object metadata entry with
// an optional Tags slice. Used by ScanObjectsGrouped tests to verify Tags
// flow end-to-end through ListObjectVersions → ScanObjectsGrouped.
func writeVersionedObjectMetaTagged(t *testing.T, b *DistributedBackend, bucket, key, versionID, etag string, size int64, tags []storage.Tag) {
	t.Helper()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
		Tags:         tags,
	})
	require.NoError(t, err)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(objectMetaKey(bucket, key), meta); err != nil {
			return err
		}
		if err := txn.Set(objectMetaKeyV(bucket, key, versionID), meta); err != nil {
			return err
		}
		return txn.Set(latestKey(bucket, key), []byte(versionID))
	}))
}

func writeLegacyObjectMetaTagged(t *testing.T, b *DistributedBackend, bucket, key, etag string, size int64, tags []storage.Tag) {
	t.Helper()
	meta, err := marshalObjectMeta(objectMeta{
		Key:          key,
		Size:         size,
		ContentType:  "application/octet-stream",
		ETag:         etag,
		LastModified: time.Now().Unix(),
		Tags:         tags,
	})
	require.NoError(t, err)
	require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
		return txn.Set(objectMetaKey(bucket, key), meta)
	}))
}

func drainObjectKeyGroups(t *testing.T, ch <-chan storage.ObjectKeyGroup) []storage.ObjectKeyGroup {
	t.Helper()
	var out []storage.ObjectKeyGroup
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		select {
		case g, ok := <-ch:
			if !ok {
				return out
			}
			// Copy Versions to defeat pool reuse caveat in scan_types.go.
			cp := append([]storage.ObjectVersionRecord(nil), g.Versions...)
			out = append(out, storage.ObjectKeyGroup{Bucket: g.Bucket, Key: g.Key, Versions: cp})
		case <-ctx.Done():
			t.Fatalf("ObjectKeyGroup channel did not close within timeout")
			return out
		}
	}
}

func drainMultipartRecords(t *testing.T, ch <-chan storage.MultipartUploadRecord) []storage.MultipartUploadRecord {
	t.Helper()
	var out []storage.MultipartUploadRecord
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for {
		select {
		case rec, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, rec)
		case <-ctx.Done():
			t.Fatalf("MultipartUploadRecord channel did not close within timeout")
			return out
		}
	}
}

func TestDistributedBackend_ScanObjectsGrouped_EmptyBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	ch, err := b.ScanObjectsGrouped("bkt")
	require.NoError(t, err)
	groups := drainObjectKeyGroups(t, ch)
	assert.Empty(t, groups)
}

func TestDistributedBackend_ScanObjectsGrouped_MissingBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.ScanObjectsGrouped("no-such-bucket")
	assert.Error(t, err)
}

func TestDistributedBackend_ScanObjectsGrouped_GroupsByKey(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	// Two versions of "a" and one of "b". UUIDv7 is lex-ASC-by-time; the
	// scan returns newest-first within a key, so v2 > v1 lexically.
	writeVersionedObjectMeta(t, b, "bkt", "a", "01AAAA0001", "etag-a-v1", 11)
	writeVersionedObjectMeta(t, b, "bkt", "a", "01AAAA0002", "etag-a-v2", 22)
	writeVersionedObjectMeta(t, b, "bkt", "b", "01BBBB0001", "etag-b-v1", 33)

	ch, err := b.ScanObjectsGrouped("bkt")
	require.NoError(t, err)
	groups := drainObjectKeyGroups(t, ch)

	require.Len(t, groups, 2)

	// Groups should arrive in key-ASC order ("a" before "b").
	assert.Equal(t, "a", groups[0].Key)
	assert.Equal(t, "bkt", groups[0].Bucket)
	require.Len(t, groups[0].Versions, 2)
	// Newest-first inside the group: VersionID DESC.
	assert.Equal(t, "01AAAA0002", groups[0].Versions[0].VersionID)
	assert.Equal(t, "etag-a-v2", groups[0].Versions[0].ETag)
	assert.True(t, groups[0].Versions[0].IsLatest, "newest version must be IsLatest=true")
	assert.Equal(t, "01AAAA0001", groups[0].Versions[1].VersionID)
	assert.False(t, groups[0].Versions[1].IsLatest, "older version must be IsLatest=false")

	assert.Equal(t, "b", groups[1].Key)
	require.Len(t, groups[1].Versions, 1)
	assert.Equal(t, "01BBBB0001", groups[1].Versions[0].VersionID)
}

func TestDistributedBackend_ScanObjectsGrouped_PassesDeleteMarkers(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	// One live version + one delete marker on the same key. Lifecycle
	// noncurrent expiration needs to see both.
	writeVersionedObjectMeta(t, b, "bkt", "k", "01AAAA0001", "etag-live", 10)
	writeVersionedObjectMeta(t, b, "bkt", "k", "01AAAA0002", deleteMarkerETag, 0)

	ch, err := b.ScanObjectsGrouped("bkt")
	require.NoError(t, err)
	groups := drainObjectKeyGroups(t, ch)

	require.Len(t, groups, 1)
	require.Len(t, groups[0].Versions, 2, "delete markers must pass through")
	// Newest is the delete marker.
	assert.True(t, groups[0].Versions[0].IsDeleteMarker)
	assert.False(t, groups[0].Versions[1].IsDeleteMarker)
}

// Phase 2 invariant: Tags written into objectMeta surface through
// ListObjectVersions and therefore through ScanObjectsGrouped.
func TestDistributedBackend_ScanObjectsGrouped_PreservesTags(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	tags := []storage.Tag{{Key: "env", Value: "prod"}, {Key: "team", Value: "data"}}
	writeVersionedObjectMetaTagged(t, b, "bkt", "tagged", "01AAAA0001", "etag", 5, tags)

	ch, err := b.ScanObjectsGrouped("bkt")
	require.NoError(t, err)
	groups := drainObjectKeyGroups(t, ch)

	require.Len(t, groups, 1)
	require.Len(t, groups[0].Versions, 1)
	assert.Equal(t, tags, groups[0].Versions[0].Tags)
}

func TestDistributedBackend_ScanObjectsGrouped_IncludesLegacyUnversionedObjects(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	tags := []storage.Tag{{Key: "expire", Value: "yes"}}
	writeLegacyObjectMetaTagged(t, b, "bkt", "folder/tagged", "etag", 42, tags)

	ch, err := b.ScanObjectsGrouped("bkt")
	require.NoError(t, err)
	groups := drainObjectKeyGroups(t, ch)

	require.Len(t, groups, 1)
	assert.Equal(t, "folder/tagged", groups[0].Key)
	require.Len(t, groups[0].Versions, 1)
	assert.Empty(t, groups[0].Versions[0].VersionID)
	assert.True(t, groups[0].Versions[0].IsLatest)
	assert.Equal(t, int64(42), groups[0].Versions[0].Size)
	assert.Equal(t, tags, groups[0].Versions[0].Tags)
}

func TestDistributedBackend_ScanLocalMultipartUploads_EmptyBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))

	ch, err := b.ScanLocalMultipartUploads("bkt")
	require.NoError(t, err)
	recs := drainMultipartRecords(t, ch)
	assert.Empty(t, recs)
}

func TestDistributedBackend_ScanLocalMultipartUploads_MissingBucket(t *testing.T) {
	b := newTestDistributedBackend(t)
	_, err := b.ScanLocalMultipartUploads("no-such-bucket")
	assert.Error(t, err)
}

func TestDistributedBackend_ScanLocalMultipartUploads_EmitsActiveUploads(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket(context.Background(), "bkt"))
	require.NoError(t, b.CreateBucket(context.Background(), "other"))

	writeMultipartMeta(t, b, "upload-aaa", clusterMultipartMeta{
		Bucket:      "bkt",
		Key:         "a.bin",
		ContentType: "application/octet-stream",
		CreatedAt:   100,
	})
	writeMultipartMeta(t, b, "upload-bbb", clusterMultipartMeta{
		Bucket:      "bkt",
		Key:         "b.bin",
		ContentType: "application/octet-stream",
		CreatedAt:   200,
	})
	// Different-bucket upload must be filtered out.
	writeMultipartMeta(t, b, "upload-other", clusterMultipartMeta{
		Bucket:      "other",
		Key:         "x.bin",
		ContentType: "application/octet-stream",
		CreatedAt:   300,
	})

	ch, err := b.ScanLocalMultipartUploads("bkt")
	require.NoError(t, err)
	recs := drainMultipartRecords(t, ch)

	require.Len(t, recs, 2)
	got := map[string]storage.MultipartUploadRecord{}
	for _, r := range recs {
		got[r.UploadID] = r
	}
	require.Contains(t, got, "upload-aaa")
	assert.Equal(t, "bkt", got["upload-aaa"].Bucket)
	assert.Equal(t, "a.bin", got["upload-aaa"].Key)
	assert.Equal(t, int64(100), got["upload-aaa"].InitiatedAt)

	require.Contains(t, got, "upload-bbb")
	assert.Equal(t, "b.bin", got["upload-bbb"].Key)
	assert.Equal(t, int64(200), got["upload-bbb"].InitiatedAt)
}
