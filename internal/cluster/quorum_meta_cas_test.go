package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestFanOutQuorumMeta_CASRejectIsAVoteNotShortCircuit verifies the fixed
// quorum primitive: a CAS reject is ONE failed vote, not a global short-circuit.
// With K-of-N replica skew, K accepting nodes win even when laggard replicas
// CAS-reject; the sentinel only surfaces when K is genuinely UNREACHABLE.
func TestFanOutQuorumMeta_CASRejectIsAVoteNotShortCircuit(t *testing.T) {
	ctx := context.Background()

	// 6 nodes, K=4: 4 accept, 2 CAS-reject (laggards). K reached → success, NOT a
	// spurious reject (the partial-publish bug this fixes).
	nodes := []string{"a", "b", "c", "d", "e", "f"}
	reject := map[string]bool{"e": true, "f": true}
	err := fanOutQuorumMeta(ctx, nodes, 4, func(_ context.Context, node string) error {
		if reject[node] {
			return errQuorumMetaCASReject
		}
		return nil
	})
	require.NoError(t, err, "K accepts must win even though laggard replicas CAS-rejected")

	// All 6 reject (genuine base-advanced-everywhere): K unreachable → surface the
	// DISTINGUISHABLE sentinel so the caller re-reads and retries.
	err = fanOutQuorumMeta(ctx, nodes, 4, func(_ context.Context, _ string) error {
		return errQuorumMetaCASReject
	})
	require.ErrorIs(t, err, errQuorumMetaCASReject, "all-reject must surface the CAS sentinel")

	// Mixed: 3 accept, 3 CAS-reject, K=4 unreachable, a reject was seen → sentinel.
	rejectHalf := map[string]bool{"d": true, "e": true, "f": true}
	err = fanOutQuorumMeta(ctx, nodes, 4, func(_ context.Context, node string) error {
		if rejectHalf[node] {
			return errQuorumMetaCASReject
		}
		return nil
	})
	require.ErrorIs(t, err, errQuorumMetaCASReject, "K-unreachable with a CAS reject surfaces the sentinel")
}

// quorumMetaWriteAccepts is the bool projection of decideQuorumMetaWrite used by
// the guard unit tests: a candidate is "accepted" only when it is APPLIED (renamed
// over the existing blob). Both a CAS reject and a no-op skip (LWW loss /
// byte-identical CAS re-delivery) are "not applied" → false.
func quorumMetaWriteAccepts(existing, cand PutObjectMetaCmd) bool {
	return decideQuorumMetaWrite(existing, cand) == quorumMetaWriteApply
}

// TestQuorumMetaWriteAccepts_CAS exercises the CAS branch: a candidate with
// MetaSeqCAS=true is accepted iff its MetaSeq is exactly existing.MetaSeq+1.
// A late (stalled-owner) write that read base=N and writes N+1 is rejected once
// a newer writer has already advanced existing to N+1 (the failover fence).
func TestQuorumMetaWriteAccepts_CAS(t *testing.T) {
	existing := PutObjectMetaCmd{MetaSeq: 5}
	require.True(t, quorumMetaWriteAccepts(existing, PutObjectMetaCmd{MetaSeqCAS: true, MetaSeq: 6}))  // base+1 wins
	require.False(t, quorumMetaWriteAccepts(existing, PutObjectMetaCmd{MetaSeqCAS: true, MetaSeq: 7})) // skips a slot
	require.False(t, quorumMetaWriteAccepts(existing, PutObjectMetaCmd{MetaSeqCAS: true, MetaSeq: 5})) // stale base, no advance
	// Failover lost-update fence: a newer writer already advanced existing to 6, so
	// a late stalled owner that read base=5 and re-offers 6 is rejected (needs 7).
	advanced := PutObjectMetaCmd{MetaSeq: 6}
	require.False(t, quorumMetaWriteAccepts(advanced, PutObjectMetaCmd{MetaSeqCAS: true, MetaSeq: 6}))
}

// TestQuorumMetaWriteAccepts_FirstWriteCAS: absent existing → MetaSeq treated as
// 0, so the first CAS write must carry MetaSeq==1.
func TestQuorumMetaWriteAccepts_FirstWriteCAS(t *testing.T) {
	var absent PutObjectMetaCmd // MetaSeq 0
	require.True(t, quorumMetaWriteAccepts(absent, PutObjectMetaCmd{MetaSeqCAS: true, MetaSeq: 1}))
	require.False(t, quorumMetaWriteAccepts(absent, PutObjectMetaCmd{MetaSeqCAS: true, MetaSeq: 2}))
	require.False(t, quorumMetaWriteAccepts(absent, PutObjectMetaCmd{MetaSeqCAS: true, MetaSeq: 0}))
}

// TestQuorumMetaWriteAccepts_LWWUnchanged: a candidate with MetaSeqCAS=false
// keeps the existing LWW semantics (ModTime > VersionID > MetaSeq).
func TestQuorumMetaWriteAccepts_LWWUnchanged(t *testing.T) {
	cur := PutObjectMetaCmd{ModTime: 100, VersionID: "v", MetaSeq: 1}
	require.True(t, quorumMetaWriteAccepts(cur, PutObjectMetaCmd{ModTime: 200}))  // newer ModTime wins (LWW)
	require.False(t, quorumMetaWriteAccepts(cur, PutObjectMetaCmd{ModTime: 50}))  // older loses
	require.False(t, quorumMetaWriteAccepts(cur, PutObjectMetaCmd{ModTime: 100})) // tie on ModTime, lower VID loses
}

// TestQuorumMetaWriteAccepts_CrossType: an LWW write whose MetaSeq matches what a
// CAS write would require is STILL routed through LWW (MetaSeqCAS=false), so it is
// decided by ModTime/VersionID/MetaSeq — NOT by the +1 CAS rule. This is the
// shared-monotonic-MetaSeq cross-type case (e.g. a tags-set with MetaSeq=N+1).
func TestQuorumMetaWriteAccepts_CrossType(t *testing.T) {
	existing := PutObjectMetaCmd{ModTime: 100, VersionID: "v", MetaSeq: 5}
	// LWW candidate with the "CAS-correct" +1 MetaSeq but an OLDER ModTime: LWW
	// rejects it (ModTime loses) even though MetaSeq==existing+1 would pass CAS.
	require.False(t, quorumMetaWriteAccepts(existing, PutObjectMetaCmd{MetaSeqCAS: false, VersionID: "v", MetaSeq: 6, ModTime: 50}))
	// LWW candidate with a newer ModTime wins regardless of MetaSeq gap.
	require.True(t, quorumMetaWriteAccepts(existing, PutObjectMetaCmd{MetaSeqCAS: false, VersionID: "v", MetaSeq: 99, ModTime: 200}))
}

// TestWriteQuorumMetaLocal_CASRejectSurfacesError verifies F3 in the latest-only
// guard: a CAS candidate (MetaSeqCAS) whose base no longer matches returns the
// DISTINGUISHABLE errQuorumMetaCASReject — NOT a silent nil — so the append /
// coalesce coordinator can retry instead of mistaking the reject for success.
func TestWriteQuorumMetaLocal_CASRejectSurfacesError(t *testing.T) {
	b := newTestDistributedBackend(t)
	base := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 3})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", base))

	// Stale CAS write: read base=2 (so offers MetaSeq=3) but existing already 3 → needs 4 → reject.
	stale := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 3, MetaSeqCAS: true})
	err := b.shardSvc.writeQuorumMetaLocal("bkt", "k", stale)
	require.ErrorIs(t, err, errQuorumMetaCASReject, "stale CAS write must surface the distinguishable reject error")

	// Existing blob is unchanged (the reject did not rename).
	got, rerr := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaSubDir, "bkt", "k"))
	require.NoError(t, rerr)
	require.Equal(t, base, got, "rejected CAS write must not overwrite the existing blob")

	// A correctly-fenced CAS write (base+1 == 4) is accepted.
	good := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 4, MetaSeqCAS: true})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", good))
}

// TestWriteQuorumMetaLocal_IdempotentCASReplayIsNoOp verifies the K-of-N
// re-delivery case: the SAME CAS candidate written to the same node twice (a
// placement set that resolves multiple node IDs to one physical node, e.g. EC
// over a single host) is a byte-identical no-op SKIP, NOT a CAS reject. Without
// this the second fan-out write would surface errQuorumMetaCASReject and fail the
// whole append/coalesce RMW.
func TestWriteQuorumMetaLocal_IdempotentCASReplayIsNoOp(t *testing.T) {
	b := newTestDistributedBackend(t)
	// First CAS write onto an absent base (MetaSeq 0 → 1) is applied.
	cand := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", cand))

	// Re-delivering the EXACT same candidate is an idempotent no-op (nil, not reject).
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", cand),
		"byte-identical CAS re-delivery must be a no-op, not errQuorumMetaCASReject")

	got, rerr := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaSubDir, "bkt", "k"))
	require.NoError(t, rerr)
	require.Equal(t, cand, got, "the blob is unchanged after the idempotent replay")

	// A DIFFERENT candidate at the SAME MetaSeq (not byte-identical) is still a
	// genuine CAS reject — idempotency must not mask a real base mismatch.
	other := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100, MetaSeq: 1, MetaSeqCAS: true, ContentType: "text/plain"})
	require.ErrorIs(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", other), errQuorumMetaCASReject,
		"a non-identical CAS candidate at the same MetaSeq must still reject")
}

// TestWriteQuorumMetaVersionLocal_CASRejectSurfacesError mirrors the F3 check on
// the per-version guard.
func TestWriteQuorumMetaVersionLocal_CASRejectSurfacesError(t *testing.T) {
	b := newTestDistributedBackend(t)
	sub := filepath.Join("k", "vid-1")
	base := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 3})
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, base))

	stale := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 3, MetaSeqCAS: true})
	err := b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, stale)
	require.ErrorIs(t, err, errQuorumMetaCASReject)

	got, rerr := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaVersionsSubDir, "bkt", "k", "vid-1"))
	require.NoError(t, rerr)
	require.Equal(t, base, got)

	good := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "vid-1", ModTime: 100, MetaSeq: 4, MetaSeqCAS: true})
	require.NoError(t, b.shardSvc.writeQuorumMetaVersionLocal("bkt", sub, good))
}

// TestWriteQuorumMetaLocal_LWWLossStaysNil verifies the complement of F3: an LWW
// loss (MetaSeqCAS=false, older ModTime) keeps the legacy SILENT nil skip — it
// must NOT be conflated with the CAS reject error.
func TestWriteQuorumMetaLocal_LWWLossStaysNil(t *testing.T) {
	b := newTestDistributedBackend(t)
	newer := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v2", ModTime: 200})
	older := mustEncodeMetaCmd(t, PutObjectMetaCmd{Bucket: "bkt", Key: "k", VersionID: "v1", ModTime: 100})
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", newer))
	require.NoError(t, b.shardSvc.writeQuorumMetaLocal("bkt", "k", older), "LWW loss must be a silent nil no-op, never an error")
	got, rerr := os.ReadFile(filepath.Join(b.shardSvc.dataDirs[0], quorumMetaSubDir, "bkt", "k"))
	require.NoError(t, rerr)
	require.Equal(t, newer, got, "LWW loser must not overwrite the newer blob")
}
