package cluster

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// ObjVersionRef identifies a specific object version for cutover reporting.
type ObjVersionRef struct {
	Bucket, Key, VersionID string
}

// objVersionRef is a package-internal alias kept for internal use.
type objVersionRef = ObjVersionRef

// cutoverReadiness summarizes per-version quorum-meta coverage for a bucket.
type cutoverReadiness struct {
	Complete int // VID found in readQuorumMetaVersionsStrict union
	Gaps     int // VID missing but placement resolvable (backfill can fix)
	Stuck    int // VID missing AND placement unresolvable
	Unknown  int // decode error or strict-readback error (fail-closed)
	Excluded int // internal bucket, appendable, or coalesced (intentionally skipped)

	GapRefs     []ObjVersionRef // capped at maxVerifyRefs
	StuckRefs   []ObjVersionRef
	UnknownRefs []ObjVersionRef
}

// maxVerifyRefs is the maximum number of per-class refs collected before capping.
const maxVerifyRefs = 100

// readQuorumMetaVersionsStrict mirrors readQuorumMetaVersions but returns an
// error on any local read/decode failure or peer RPC error, rather than
// silently skipping. The verifier uses this to fail-closed to Unknown instead
// of silently treating an unreadable node as "no versions here".
//
// Known limitation: a per-version blob that is physically present but contains
// a corrupt payload is misclassified as GAP or STUCK rather than UNKNOWN.
// readQuorumMetaVersionsLocal and the RPC client both drop per-blob decode
// failures internally before this function sees the results, so a corrupt-but-
// present blob appears as "VID absent". This is SAFE — GAP and STUCK are both
// not-ready states, so no false-READY signal is produced — but it is less
// precise than UNKNOWN would be. Fixing this would require propagating per-blob
// decode errors up through the local reader and RPC path.
func (b *DistributedBackend) readQuorumMetaVersionsStrict(bucket, key string) ([]PutObjectMetaCmd, error) {
	if b.shardSvc == nil {
		return nil, fmt.Errorf("readQuorumMetaVersionsStrict: no shard service")
	}
	byVID := map[string]PutObjectMetaCmd{}
	put := func(c PutObjectMetaCmd) {
		if ex, ok := byVID[c.VersionID]; !ok || c.MetaSeq >= ex.MetaSeq {
			byVID[c.VersionID] = c
		}
	}

	// Local read — fail on error.
	local, err := b.shardSvc.readQuorumMetaVersionsLocal(bucket, key)
	if err != nil {
		return nil, fmt.Errorf("readQuorumMetaVersionsStrict local %s/%s: %w", bucket, key, err)
	}
	for _, c := range local {
		put(c)
	}

	self := b.currentSelfAddr()
	seen := map[string]bool{self: true}
	ctx, cancel := context.WithTimeout(context.Background(), quorumMetaReadTimeout)
	defer cancel()

	if b.shardGroup != nil {
		for _, g := range b.shardGroup.ShardGroups() {
			for _, p := range g.PeerIDs {
				if seen[p] {
					continue
				}
				seen[p] = true
				addr, aerr := b.shardSvc.resolvePeerAddress(p)
				if aerr != nil {
					return nil, fmt.Errorf("readQuorumMetaVersionsStrict resolve peer %s: %w", p, aerr)
				}
				remote, rerr := b.shardSvc.ReadQuorumMetaVersions(ctx, addr, bucket, key)
				if rerr != nil {
					return nil, fmt.Errorf("readQuorumMetaVersionsStrict peer %s: %w", p, rerr)
				}
				for _, c := range remote {
					put(c)
				}
			}
		}
	}

	out := make([]PutObjectMetaCmd, 0, len(byVID))
	for _, c := range byVID {
		out = append(out, c)
	}
	return out, nil
}

// CutoverReadiness summarizes per-version quorum-meta coverage for a bucket.
// It is the public alias for cutoverReadiness returned by VerifyPerVersionCutover.
type CutoverReadiness = cutoverReadiness

// VerifyPerVersionCutover is the exported entry point for the S4a cutover gate.
// It delegates to the internal verifyPerVersionCutover implementation.
func (b *DistributedBackend) VerifyPerVersionCutover(bucket string) (CutoverReadiness, error) {
	return b.verifyPerVersionCutover(bucket)
}

// verifyPerVersionCutover checks whether every versioned non-appendable object
// version in bucket has a readable per-version quorum-meta blob (the S4 cutover
// safety precondition). It is read-only: no writes, no deletes.
//
// Classification:
//   - Complete: VID found in readQuorumMetaVersionsStrict union AND the decoded
//     cmd yields a readable layout (segments present, EC-resolvable, or delete
//     marker). A blob that is present but carries an unresolvable layout is
//     classified Unknown (fail-closed) — it would 404 after cutover.
//   - Gap:      VID missing AND placement is resolvable (backfill can fix).
//   - Stuck:    VID missing AND placement is unresolvable (no NodeIDs).
//   - Unknown:  decode error, strict-readback error, or present-but-unreadable
//     layout (fail-closed).
//   - Excluded: internal bucket, appendable, or coalesced (intentionally skipped).
//
// Fast-path: internal buckets → count all FSM obj: records as Excluded.
// Non-versioning-enabled buckets → return zero readiness (nothing to verify).
//
// Scope: this gate validates the per-version-blob fallback path for
// versioning-enabled, non-internal, non-appendable objects only. The future S4
// cutover slice MUST NOT remove any FSM fallback not covered here (specifically:
// non-versioned latest-only-blob objects (headObjectMeta / object_get.go ~:224)
// and appendable/coalesced objects) without first extending this verifier —
// otherwise "all nodes report 0 gaps" would be a false-ready signal for those
// object classes.
func (b *DistributedBackend) verifyPerVersionCutover(bucket string) (cutoverReadiness, error) {
	var r cutoverReadiness

	addRef := func(refs *[]objVersionRef, key, vid string) {
		if len(*refs) < maxVerifyRefs {
			*refs = append(*refs, objVersionRef{Bucket: bucket, Key: key, VersionID: vid})
		}
	}

	// Internal buckets are FSM-authoritative; they never use per-version blobs.
	if storage.IsInternalBucket(bucket) {
		_ = b.forEachHostedObjVersion(bucket, func(_ *DistributedBackend, _, _ string, _ objectMeta, _ error) error {
			r.Excluded++
			return nil
		})
		return r, nil
	}

	ctx := context.Background()
	if !b.bucketVersioningEnabled(ctx, bucket) {
		return r, nil
	}

	// Per-key memoization: one readQuorumMetaVersionsStrict call per key.
	// Stores VID → decoded PutObjectMetaCmd so the layout check uses the
	// already-decoded cmd without an extra RPC.
	type keyResult struct {
		cmds map[string]PutObjectMetaCmd // VID → decoded cmd
		err  error
	}
	keyCache := map[string]*keyResult{}

	err := b.forEachHostedObjVersion(bucket, func(gb *DistributedBackend, key, vid string, meta objectMeta, decodeErr error) error {
		if decodeErr != nil {
			r.Unknown++
			addRef(&r.UnknownRefs, key, vid)
			return nil
		}

		// Appendable/coalesced: excluded from cutover verification (FSM-authoritative).
		if meta.IsAppendable || len(meta.Coalesced) > 0 {
			r.Excluded++
			return nil
		}

		// Real versioned non-appendable record: check quorum-meta coverage.
		kr, cached := keyCache[key]
		if !cached {
			strictCmds, qerr := b.readQuorumMetaVersionsStrict(bucket, key)
			kr = &keyResult{cmds: map[string]PutObjectMetaCmd{}, err: qerr}
			if qerr == nil {
				for _, c := range strictCmds {
					kr.cmds[c.VersionID] = c
				}
			}
			keyCache[key] = kr
		}
		if kr.err != nil {
			r.Unknown++
			addRef(&r.UnknownRefs, key, vid)
			return nil
		}
		if cmd, found := kr.cmds[vid]; found {
			// VID is present in the strict readback union. Verify the decoded cmd
			// yields a readable layout — matching getObjectVersionCtx's dispatch:
			//   1. Delete marker (IsDeleteMarker) → COMPLETE (caller gets 405, not 404).
			//   2. Non-zero Segments → SegmentReader path → COMPLETE.
			//   3. EC: NodeIDs non-empty AND ECData > 0 AND len(NodeIDs)==ECData+ECParity
			//      (same predicate as ResolvePlacement) → COMPLETE.
			//   Otherwise → present blob but unreadable layout → UNKNOWN (fail-closed).
			if cmd.IsDeleteMarker {
				r.Complete++
				return nil
			}
			if len(cmd.Segments) > 0 {
				r.Complete++
				return nil
			}
			if len(cmd.NodeIDs) > 0 && cmd.ECData > 0 && len(cmd.NodeIDs) == int(cmd.ECData)+int(cmd.ECParity) {
				r.Complete++
				return nil
			}
			// Present but no readable layout — would 404 after cutover.
			r.Unknown++
			addRef(&r.UnknownRefs, key, vid)
			return nil
		}

		// VID missing from quorum-meta: classify by placement resolvability.
		// A delete marker with empty NodeIDs can have placement derived; a live
		// object with empty NodeIDs has nowhere to write → Stuck.
		hasPlacement := len(meta.NodeIDs) > 0
		if !hasPlacement && meta.ETag == deleteMarkerETag {
			nodeIDs, _ := deriveMarkerPlacement(gb, key)
			hasPlacement = len(nodeIDs) > 0
		}
		if hasPlacement {
			r.Gaps++
			addRef(&r.GapRefs, key, vid)
		} else {
			r.Stuck++
			addRef(&r.StuckRefs, key, vid)
		}
		return nil
	})
	if err != nil {
		return r, fmt.Errorf("verifyPerVersionCutover bucket %s: %w", bucket, err)
	}
	return r, nil
}

// ListCutoverBuckets returns the union of all locally-hosted generation
// groups' buckets. Implements scrubber.PerVersionCutoverVerifiable.
// Mirrors ListBackfillBuckets (delegates to SegmentSweepBuckets).
func (b *DistributedBackend) ListCutoverBuckets(ctx context.Context) ([]string, error) {
	return b.SegmentSweepBuckets(ctx)
}

// VerifyBucketCutover runs the read-only per-version coverage check for bucket
// and returns a scrubber.CutoverReadiness tally. Implements
// scrubber.PerVersionCutoverVerifiable.
func (b *DistributedBackend) VerifyBucketCutover(_ context.Context, bucket string) (scrubber.CutoverReadiness, error) {
	r, err := b.verifyPerVersionCutover(bucket)
	if err != nil {
		return scrubber.CutoverReadiness{}, err
	}
	return scrubber.CutoverReadiness{
		Complete: r.Complete,
		Gaps:     r.Gaps,
		Stuck:    r.Stuck,
		Unknown:  r.Unknown,
		Excluded: r.Excluded,
	}, nil
}
