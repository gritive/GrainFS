package packblob

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// newRewrapTestKeeper builds a fresh DEKKeeper at gen 0 with the standard test
// cluster ID.
func newRewrapTestKeeper(t testing.TB) (*encrypt.DEKKeeper, []byte) {
	t.Helper()
	cid := packblobTestClusterID()
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x66}, encrypt.KEKSize), cid)
	require.NoError(t, err)
	return keeper, cid
}

// newRewrapTestBackend builds a DEK-backed PackedBackend over a fresh temp dir.
func newRewrapTestBackend(t testing.TB, keeper *encrypt.DEKKeeper, cid []byte) *PackedBackend {
	t.Helper()
	dir := t.TempDir()
	inner, err := storage.NewLocalBackend(dir + "/local")
	require.NoError(t, err)
	pb, err := NewPackedBackendWithOptions(inner, dir+"/blobs", 64*1024, PackedBackendOptions{
		DEKKeeper: keeper,
		ClusterID: cid,
		Compress:  true,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = pb.Close()
		_ = inner.Close()
	})
	return pb
}

// TestPackblobReadWithGen reports the seal gen for the entry: gen 0 before
// rotation, gen 1 after.
func TestPackblobReadWithGen(t *testing.T) {
	dir := t.TempDir()
	keeper, _ := newRewrapTestKeeper(t)
	bs := newPackblobDEKStoreSharedKeeper(t, dir, 256*1024*1024, keeper)
	defer bs.Close()

	require.Equal(t, uint32(0), keeper.ActiveDEKGeneration())
	dataA := []byte("entry sealed at gen 0")
	locA, err := bs.Append("bucket/A", dataA)
	require.NoError(t, err)

	gotA, genA, err := bs.ReadWithGen(locA)
	require.NoError(t, err)
	assert.Equal(t, dataA, gotA)
	assert.Equal(t, uint32(0), genA)

	require.NoError(t, keeper.Rotate())
	require.Equal(t, uint32(1), keeper.ActiveDEKGeneration())
	dataB := []byte("entry sealed at gen 1")
	locB, err := bs.Append("bucket/B", dataB)
	require.NoError(t, err)

	gotB, genB, err := bs.ReadWithGen(locB)
	require.NoError(t, err)
	assert.Equal(t, dataB, gotB)
	assert.Equal(t, uint32(1), genB)

	// gen-0 entry still reports gen 0 after rotation.
	gotA2, genA2, err := bs.ReadWithGen(locA)
	require.NoError(t, err)
	assert.Equal(t, dataA, gotA2)
	assert.Equal(t, uint32(0), genA2)
}

// TestPackblobRewrapStaleEntries sweeps every live entry onto the active gen,
// is migration-only (content + ETag preserved), and is idempotent.
func TestPackblobRewrapStaleEntries(t *testing.T) {
	keeper, cid := newRewrapTestKeeper(t)
	pb := newRewrapTestBackend(t, keeper, cid)
	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "b"))

	contents := map[string]string{
		"k1": "alpha payload",
		"k2": "bravo payload",
		"k3": "charlie payload",
	}
	for k, v := range contents {
		_, err := pb.PutObject(ctx, "b", k, strings.NewReader(v), "text/plain")
		require.NoError(t, err)
	}

	// All entries sealed at gen 0.
	require.NoError(t, keeper.Rotate())
	require.Equal(t, uint32(1), keeper.ActiveDEKGeneration())

	n, err := pb.RewrapStaleEntries(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, len(contents), n, "all live entries migrated")

	// Every object still GETs identical bytes, and its on-disk seal is now gen 1.
	for k, v := range contents {
		rc, _, err := pb.GetObject(ctx, "b", k)
		require.NoError(t, err)
		got := new(bytes.Buffer)
		_, err = got.ReadFrom(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		assert.Equal(t, v, got.String(), "content preserved for %s", k)

		pk := packedKey{bucket: "b", key: k}
		v2, ok := pb.index.Load(pk)
		require.True(t, ok)
		_, gen, err := pb.blobStore.ReadWithGen(v2.(*indexEntry).Location)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), gen, "entry %s re-sealed at active gen", k)
	}

	// Idempotent: nothing stale remains.
	n2, err := pb.RewrapStaleEntries(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, 0, n2, "second sweep is a no-op")
}

// TestPackblobRewrapLane_Counter: the lane delegates to RewrapStaleEntries and
// increments the per-active-gen counter by the rewrapped count.
func TestPackblobRewrapLane_Counter(t *testing.T) {
	keeper, cid := newRewrapTestKeeper(t)
	pb := newRewrapTestBackend(t, keeper, cid)
	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "b"))

	for i := 0; i < 4; i++ {
		_, err := pb.PutObject(ctx, "b", fmt.Sprintf("k%d", i), strings.NewReader("payload"), "text/plain")
		require.NoError(t, err)
	}
	require.NoError(t, keeper.Rotate())

	lane := NewPackblobRewrapLane(pb)
	assert.Equal(t, "packblob", lane.Name())

	before := testutil.ToFloat64(RewrapPackblobEntriesTotal.WithLabelValues("1"))
	require.NoError(t, lane.RewrapByGen(ctx, 0, 1))
	after := testutil.ToFloat64(RewrapPackblobEntriesTotal.WithLabelValues("1"))
	assert.Equal(t, float64(4), after-before, "counter advanced by rewrapped count")
}

// TestPackblobRewrapLane_NilPB: a nil-pb lane is a no-op (the wiring task
// nil-gates the lane on single-node-only stores).
func TestPackblobRewrapLane_NilPB(t *testing.T) {
	lane := NewPackblobRewrapLane(nil)
	require.NoError(t, lane.RewrapByGen(context.Background(), 0, 1))
}

// TestPackblobRewrap_ConcurrentPut (MANDATORY -race deliverable): a rewrap sweep
// runs while a key is concurrently PUT-overwritten with NEW content. The PUT's
// Swap publishes a fresh *indexEntry pointer, so rewrap's CAS on the old pointer
// loses and skips it — the latest PUT content must win, and no other key is
// corrupted.
func TestPackblobRewrap_ConcurrentPut(t *testing.T) {
	keeper, cid := newRewrapTestKeeper(t)
	pb := newRewrapTestBackend(t, keeper, cid)
	ctx := context.Background()
	require.NoError(t, pb.CreateBucket(ctx, "b"))

	// Stable keys: written once, never overwritten; must survive intact.
	stable := map[string]string{
		"s1": "stable one",
		"s2": "stable two",
		"s3": "stable three",
	}
	for k, v := range stable {
		_, err := pb.PutObject(ctx, "b", k, strings.NewReader(v), "text/plain")
		require.NoError(t, err)
	}
	// The hot key, overwritten concurrently.
	_, err := pb.PutObject(ctx, "b", "hot", strings.NewReader("initial"), "text/plain")
	require.NoError(t, err)

	require.NoError(t, keeper.Rotate())

	const iterations = 64
	lastContent := fmt.Sprintf("hot-v%d", iterations-1)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, perr := pb.PutObject(ctx, "b", "hot", strings.NewReader(fmt.Sprintf("hot-v%d", i)), "text/plain")
			require.NoError(t, perr)
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_, rerr := pb.RewrapStaleEntries(ctx, 1)
			require.NoError(t, rerr)
		}
	}()
	wg.Wait()

	// The hot key must read the final PUT content (rewrap CAS lost → skipped).
	rc, _, err := pb.GetObject(ctx, "b", "hot")
	require.NoError(t, err)
	got := new(bytes.Buffer)
	_, err = got.ReadFrom(rc)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
	assert.Equal(t, lastContent, got.String(), "latest PUT content must win over rewrap")

	// No stable key corrupted.
	for k, v := range stable {
		rc, _, err := pb.GetObject(ctx, "b", k)
		require.NoError(t, err)
		buf := new(bytes.Buffer)
		_, err = buf.ReadFrom(rc)
		require.NoError(t, err)
		require.NoError(t, rc.Close())
		assert.Equal(t, v, buf.String(), "stable key %s must be intact", k)
	}
}
