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

	"github.com/gritive/GrainFS/internal/scrubber"
)

// Compile-time proof that DistributedBackend satisfies scrubber.Scrubbable.
var _ scrubber.Scrubbable = (*DistributedBackend)(nil)

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
	b.SetECConfig(ECConfig{DataShards: k, ParityShards: m, Enabled: true})
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
	require.NoError(t, b.CreateBucket("bkt"))

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

func TestScanObjects_ECDisabledReturnsEmpty(t *testing.T) {
	b := newTestDistributedBackend(t)
	require.NoError(t, b.CreateBucket("bkt"))
	writeVersionedObjectMeta(t, b, "bkt", "k", "01HZXYZABC", "etag1", 42)

	// EC disabled: no shards to scrub.
	ch, err := b.ScanObjects("bkt")
	require.NoError(t, err)
	recs := drainObjectRecords(t, ch)
	assert.Empty(t, recs, "ScanObjects must be a no-op when EC is not active")
}

func TestScanObjects_SingleObject(t *testing.T) {
	b := newTestDistributedBackend(t)
	enableECForTest(t, b, 2, 1)
	require.NoError(t, b.CreateBucket("bkt"))

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
	require.NoError(t, b.CreateBucket("bkt"))

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
	require.NoError(t, b.CreateBucket("bkt"))

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
	require.NoError(t, b.CreateBucket("bkt"))
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
	assert.Equal(t, "payload", string(data))
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
