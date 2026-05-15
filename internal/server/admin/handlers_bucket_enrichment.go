package admin

import (
	"context"
	"errors"
	"sort"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/storage"
)

func userBucketNames(names []string) []string {
	filtered := make([]string, 0, len(names))
	for _, n := range names {
		if !storage.IsInternalBucket(n) {
			filtered = append(filtered, n)
		}
	}
	sort.Strings(filtered)
	return filtered
}

func bucketUpstreamSet(ctx context.Context, d *Deps) map[string]bool {
	hasUpstream := map[string]bool{}
	if d.IAM == nil {
		return hasUpstream
	}
	upstreams, err := d.IAM.ListBucketUpstreams(ctx)
	if err != nil {
		return hasUpstream
	}
	for _, u := range upstreams {
		hasUpstream[u.Bucket] = true
	}
	return hasUpstream
}

func bucketListInfo(names []string, hasUpstream map[string]bool) []BucketInfo {
	out := make([]BucketInfo, len(names))
	for i, n := range names {
		out[i] = BucketInfo{Name: n, HasUpstream: hasUpstream[n]}
	}
	return out
}

func annotateBucketUpstream(ctx context.Context, d *Deps, info *BucketInfo) error {
	if d.IAM == nil {
		return nil
	}
	if _, err := d.IAM.GetBucketUpstream(ctx, info.Name); err == nil {
		info.HasUpstream = true
		return nil
	} else if !adminapi.IsCode(err, "not_found") {
		return NewInternal("get bucket upstream: " + err.Error())
	}
	return nil
}

func annotateBucketVersioning(d *Deps, info *BucketInfo) error {
	versioning, err := d.Buckets.GetBucketVersioning(info.Name)
	if err == nil {
		info.Versioning = versioning
		return nil
	}
	if errors.Is(err, storage.ErrUnsupportedOperation) {
		return nil
	}
	return NewInternal("get bucket versioning: " + err.Error())
}
