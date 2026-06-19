package serveruntime

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/server"
)

// maxAggregatedRefs is the global cap on each aggregated ref list in the
// all-buckets response. Per-bucket lists are capped at cluster.maxVerifyRefs
// (100); across numBuckets×100 strings the JSON response can grow unbounded.
// This cap keeps it manageable regardless of bucket count.
const maxAggregatedRefs = 100

// makeVerifyPerVersionCutoverFunc builds the server-injected closure that
// aggregates per-version quorum-meta coverage across this node's hosted-group
// buckets (S4a cutover gate). Returns nil when distBackend is absent
// (single-node / no cluster), which the handler maps to 503.
func makeVerifyPerVersionCutoverFunc(b *cluster.DistributedBackend) server.VerifyPerVersionCutoverFunc {
	if b == nil {
		return nil
	}
	return func(ctx context.Context, bucket string) (server.PerVersionCutoverReadiness, error) {
		if bucket != "" {
			// Single-bucket scan.
			r, err := b.VerifyPerVersionCutover(bucket)
			if err != nil {
				return server.PerVersionCutoverReadiness{}, fmt.Errorf("verify bucket %s: %w", bucket, err)
			}
			return toServerReadiness(r), nil
		}

		// All hosted buckets.
		buckets, err := b.ListCutoverBuckets(ctx)
		if err != nil {
			return server.PerVersionCutoverReadiness{}, fmt.Errorf("list cutover buckets: %w", err)
		}

		var total server.PerVersionCutoverReadiness
		for _, bkt := range buckets {
			r, verr := b.VerifyPerVersionCutover(bkt)
			if verr != nil {
				return server.PerVersionCutoverReadiness{}, fmt.Errorf("verify bucket %s: %w", bkt, verr)
			}
			total.Complete += r.Complete
			total.Gaps += r.Gaps
			total.Stuck += r.Stuck
			total.Unknown += r.Unknown
			total.Excluded += r.Excluded
			total.Ineligible += r.Ineligible
			total.GapRefs = capAppendRefStrings(total.GapRefs, r.GapRefs, maxAggregatedRefs)
			total.StuckRefs = capAppendRefStrings(total.StuckRefs, r.StuckRefs, maxAggregatedRefs)
			total.UnknownRefs = capAppendRefStrings(total.UnknownRefs, r.UnknownRefs, maxAggregatedRefs)
		}
		return total, nil
	}
}

// toServerReadiness converts a cluster.CutoverReadiness to the server wire type.
func toServerReadiness(r cluster.CutoverReadiness) server.PerVersionCutoverReadiness {
	return server.PerVersionCutoverReadiness{
		Complete:    r.Complete,
		Gaps:        r.Gaps,
		Stuck:       r.Stuck,
		Unknown:     r.Unknown,
		Excluded:    r.Excluded,
		Ineligible:  r.Ineligible,
		GapRefs:     refSliceToStrings(r.GapRefs),
		StuckRefs:   refSliceToStrings(r.StuckRefs),
		UnknownRefs: refSliceToStrings(r.UnknownRefs),
	}
}

// refSliceToStrings converts []ObjVersionRef to []string "bucket/key@vid".
func refSliceToStrings(refs []cluster.ObjVersionRef) []string {
	if len(refs) == 0 {
		return nil
	}
	out := make([]string, len(refs))
	for i, ref := range refs {
		out[i] = ref.Bucket + "/" + ref.Key + "@" + ref.VersionID
	}
	return out
}

// capAppendRefStrings appends ref strings from src to dst up to globalCap total.
// Once dst reaches globalCap, additional refs are silently dropped so the
// aggregated response stays bounded across many buckets.
func capAppendRefStrings(dst []string, refs []cluster.ObjVersionRef, globalCap int) []string {
	for _, ref := range refs {
		if len(dst) >= globalCap {
			break
		}
		dst = append(dst, ref.Bucket+"/"+ref.Key+"@"+ref.VersionID)
	}
	return dst
}
