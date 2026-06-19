package cluster

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// writeVersionBlob writes a per-version quorum-meta blob (an encoded
// PutObjectMetaCmd) to this node's local .quorum_meta_versions subtree and
// back-dates its mtime so the age gate treats it as old (backdate > floor).
func writeVersionBlob(t *testing.T, b *DistributedBackend, bucket, key, versionID string, backdate time.Duration) string {
	t.Helper()
	blob, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:    bucket,
		Key:       key,
		VersionID: versionID,
		ECData:    1,
		NodeIDs:   []string{b.currentSelfAddr()},
	})
	require.NoError(t, err)
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal(bucket, filepath.Join(key, versionID), blob, 0))
	target := filepath.Join(b.shardSvc.DataDirs()[0], quorumMetaVersionsSubDir, bucket, key, versionID)
	past := time.Now().Add(-backdate)
	require.NoError(t, os.Chtimes(target, past, past))
	return target
}

func TestVersionRecordExistsAllHosted(t *testing.T) {
	tests := []struct {
		name        string
		put         func(b *DistributedBackend)
		wantExists  bool
		wantCertain bool
	}{
		{
			name:        "absent everywhere → certainly orphan",
			put:         func(b *DistributedBackend) {},
			wantExists:  false,
			wantCertain: true,
		},
		{
			name: "live record present → keep",
			put: func(b *DistributedBackend) {
				putObjMeta(t, b, "bkt", "k", "v1", "etag-live")
			},
			wantExists:  true,
			wantCertain: true,
		},
		{
			name: "delete-marker record present → still keep (legitimate soft-delete version)",
			put: func(b *DistributedBackend) {
				putObjMeta(t, b, "bkt", "k", "v1", deleteMarkerETag)
			},
			wantExists:  true,
			wantCertain: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := orphanWalkerBackend(t)
			tt.put(b)
			exists, certain := b.versionRecordExistsAllHosted("bkt", "k", "v1")
			require.Equal(t, tt.wantExists, exists, "exists")
			require.Equal(t, tt.wantCertain, certain, "certain")
		})
	}
}

func collectOrphanVersions(t *testing.T, b *DistributedBackend) []string {
	t.Helper()
	var got []string
	require.NoError(t, b.WalkOrphanQuorumMetaVersions(func(_, _, versionID, _ string) error {
		got = append(got, versionID)
		return nil
	}))
	return got
}

func TestWalkOrphanQuorumMetaVersions(t *testing.T) {
	b := orphanWalkerBackend(t)

	// blob A: dangling (no FSM record) and old → orphan candidate.
	writeVersionBlob(t, b, "bkt", "k", "vA", oldEnough)
	// blob B: live (FSM record present) → must NOT be yielded.
	writeVersionBlob(t, b, "bkt", "k", "vB", oldEnough)
	putObjMeta(t, b, "bkt", "k", "vB", "etag-live")
	// blob C: dangling but fresh (within age floor) → must NOT be yielded.
	writeVersionBlob(t, b, "bkt", "k", "vC", 0)

	got := collectOrphanVersions(t, b)
	require.Equal(t, []string{"vA"}, got)
}

func TestWalkOrphanQuorumMetaVersions_GateClosed_NoOp(t *testing.T) {
	b := orphanWalkerBackend(t)
	b.SetOrphanShardSweepGate(func() bool { return false }) // gate closed → fail-closed
	writeVersionBlob(t, b, "bkt", "k", "vA", oldEnough)
	require.Empty(t, collectOrphanVersions(t, b))
}

func TestDeleteOrphanQuorumMetaVersion_RemovesLocalBlob(t *testing.T) {
	b := orphanWalkerBackend(t)
	target := writeVersionBlob(t, b, "bkt", "k", "vA", oldEnough)
	require.FileExists(t, target)

	require.NoError(t, b.DeleteOrphanQuorumMetaVersion("bkt", "k", "vA"))
	_, err := os.Stat(target)
	require.True(t, os.IsNotExist(err), "blob should be gone")
}

func TestDeleteOrphanQuorumMetaVersion_ReconfirmKeepsLive(t *testing.T) {
	b := orphanWalkerBackend(t)
	target := writeVersionBlob(t, b, "bkt", "k", "vA", oldEnough)
	putObjMeta(t, b, "bkt", "k", "vA", "etag-live") // version came back to life between walk and delete

	require.NoError(t, b.DeleteOrphanQuorumMetaVersion("bkt", "k", "vA"))
	require.FileExists(t, target, "reconfirm must keep a now-live version's blob")
}

// TestPerVersionOrphanReconcile_StopsDeriveByScanResurface is the integration
// test: a dangling blob (FSM record gone — the lingering residual of a partially
// failed S2a hard-delete) makes the derive-by-scan source resurface a dead
// version; reclaiming it stops the resurface while the live version is untouched.
// The two-cycle tombstone delay is covered separately in the scrubber unit tests;
// here a single reconcile pass exercises the real walk+delete against the backend.
func TestPerVersionOrphanReconcile_StopsDeriveByScanResurface(t *testing.T) {
	b := orphanWalkerBackend(t)
	// vLive: FSM record + blob (a live retained version).
	writeVersionBlob(t, b, "bkt", "k", "vLive", oldEnough)
	putObjMeta(t, b, "bkt", "k", "vLive", "etag-live")
	// vDead: blob only — FSM record gone after a hard-delete, copy lingered.
	writeVersionBlob(t, b, "bkt", "k", "vDead", oldEnough)

	// derive-by-scan (the LIST/HEAD?vid source) currently sees BOTH → resurface.
	before, err := b.readQuorumMetaVersions("bkt", "k")
	require.NoError(t, err)
	require.Len(t, before, 2, "dangling blob resurfaces the dead version pre-reclaim")

	var cands [][3]string
	require.NoError(t, b.WalkOrphanQuorumMetaVersions(func(bk, k, v, _ string) error {
		cands = append(cands, [3]string{bk, k, v})
		return nil
	}))
	for _, c := range cands {
		require.NoError(t, b.DeleteOrphanQuorumMetaVersion(c[0], c[1], c[2]))
	}

	after, err := b.readQuorumMetaVersions("bkt", "k")
	require.NoError(t, err)
	require.Len(t, after, 1, "reclaim removes the dead version from derive-by-scan")
	require.Equal(t, "vLive", after[0].VersionID)
}
