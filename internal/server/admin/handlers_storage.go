package admin

import (
	"context"
	"sort"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/storage"
)

// AdminListStorageBuckets returns dashboard-safe bucket state without object
// counts, so the data-plane UI route cannot trigger an O(N objects) scan.
func AdminListStorageBuckets(ctx context.Context, d *Deps) (ListStorageBucketsResp, error) {
	if d.Buckets == nil {
		return ListStorageBucketsResp{}, NewUnsupported("bucket service not wired", nil)
	}

	names, err := d.Buckets.ListBuckets(ctx)
	if err != nil {
		return ListStorageBucketsResp{}, NewInternal("list buckets: " + err.Error())
	}

	hasUpstream := map[string]bool{}
	if d.IAM != nil {
		upstreams, err := d.IAM.ListBucketUpstreams(ctx)
		if err != nil && !adminapi.IsCode(err, "not_found") {
			return ListStorageBucketsResp{}, NewInternal("list bucket upstreams: " + err.Error())
		}
		for _, u := range upstreams {
			hasUpstream[u.Bucket] = true
		}
	}

	sort.Strings(names)
	out := make([]StorageBucketSummary, 0, len(names))
	for _, name := range names {
		if storage.IsInternalBucket(name) {
			continue
		}
		item := StorageBucketSummary{
			Name:        name,
			HasUpstream: hasUpstream[name],
		}
		if d.NfsExports != nil {
			if info, ok := d.NfsExports.Get(name); ok {
				item.NFSExport = &StorageBucketNFSExport{
					Registered: true,
					ReadOnly:   info.ReadOnly,
					Generation: info.Generation,
				}
			}
		}
		out = append(out, item)
	}

	return ListStorageBucketsResp{Buckets: out}, nil
}

func AdminCreateStorageBucket(ctx context.Context, d *Deps, req CreateBucketAdminReq) (BucketInfo, error) {
	return AdminCreateBucket(ctx, d, req)
}

func AdminStorageProtocols(_ context.Context, d *Deps) (StorageProtocolStatusResp, error) {
	return d.Protocols, nil
}
