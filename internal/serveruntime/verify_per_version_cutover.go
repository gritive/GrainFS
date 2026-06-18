package serveruntime

import (
	"context"
	"fmt"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/server"
)

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
			total.GapRefs = appendRefStrings(total.GapRefs, r.GapRefs)
			total.StuckRefs = appendRefStrings(total.StuckRefs, r.StuckRefs)
			total.UnknownRefs = appendRefStrings(total.UnknownRefs, r.UnknownRefs)
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

// appendRefStrings appends ref strings from src to dst (already strings).
func appendRefStrings(dst []string, refs []cluster.ObjVersionRef) []string {
	return append(dst, refSliceToStrings(refs)...)
}
