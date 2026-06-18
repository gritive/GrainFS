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
//   - Complete: VID found in readQuorumMetaVersionsStrict union.
//   - Gap:      VID missing AND placement is resolvable (backfill can fix).
//   - Stuck:    VID missing AND placement is unresolvable (no NodeIDs).
//   - Unknown:  decode error or strict-readback error (fail-closed).
//   - Excluded: internal bucket, appendable, or coalesced (intentionally skipped).
//
// Fast-path: internal buckets → count all FSM obj: records as Excluded.
// Non-versioning-enabled buckets → return zero readiness (nothing to verify).
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
	type keyResult struct {
		vids map[string]bool
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
			cmds, qerr := b.readQuorumMetaVersionsStrict(bucket, key)
			kr = &keyResult{vids: map[string]bool{}, err: qerr}
			if qerr == nil {
				for _, c := range cmds {
					kr.vids[c.VersionID] = true
				}
			}
			keyCache[key] = kr
		}
		if kr.err != nil {
			r.Unknown++
			addRef(&r.UnknownRefs, key, vid)
			return nil
		}
		if kr.vids[vid] {
			r.Complete++
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
