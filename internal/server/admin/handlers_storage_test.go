package admin_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/server/admin"
)

func TestAdminListStorageBucketsIncludesNFSExportState(t *testing.T) {
	d := &admin.Deps{
		Buckets: newFakeBucketOps(),
		IAM: &fakeIAMUpstream{
			upstreams: []iam.BucketUpstreamItem{{Bucket: "logs"}},
		},
		NfsExports: &fakeStorageNfsExports{
			exports: map[string]admin.NfsExportInfo{
				"logs": {Bucket: "logs", ReadOnly: true, Generation: 3},
			},
		},
	}
	require.NoError(t, d.Buckets.CreateBucket(context.Background(), "logs"))

	resp, err := admin.AdminListStorageBuckets(context.Background(), d)
	require.NoError(t, err)
	require.Len(t, resp.Buckets, 1)

	got := resp.Buckets[0]
	require.Equal(t, "logs", got.Name)
	require.True(t, got.HasUpstream)
	require.NotNil(t, got.NFSExport)
	require.True(t, got.NFSExport.Registered)
	require.True(t, got.NFSExport.ReadOnly)
	require.Equal(t, uint64(3), got.NFSExport.Generation)
}

func TestAdminStorageProtocolsFromDeps(t *testing.T) {
	resp, err := admin.AdminStorageProtocols(context.Background(), &admin.Deps{
		Protocols: adminapi.StorageProtocolStatusResp{
			NFS4: adminapi.ProtocolEndpointStatus{Enabled: true, Port: 2049},
			NBD:  adminapi.ProtocolEndpointStatus{Enabled: false},
			P9:   adminapi.ProtocolEndpointStatus{Enabled: true, Bind: "127.0.0.1", Port: 564},
		},
	})
	require.NoError(t, err)
	require.True(t, resp.NFS4.Enabled)
	require.Equal(t, 2049, resp.NFS4.Port)
	require.False(t, resp.NBD.Enabled)
	require.True(t, resp.P9.Enabled)
	require.Equal(t, "127.0.0.1", resp.P9.Bind)
	require.Equal(t, 564, resp.P9.Port)
}

type fakeStorageNfsExports struct {
	exports map[string]admin.NfsExportInfo
}

func (f *fakeStorageNfsExports) Upsert(context.Context, string, admin.NfsExportUpsertParams) error {
	return nil
}

func (f *fakeStorageNfsExports) Create(context.Context, string, admin.NfsExportUpsertParams) error {
	return nil
}

func (f *fakeStorageNfsExports) Delete(context.Context, string) error {
	return nil
}

func (f *fakeStorageNfsExports) DeleteForBucketDelete(context.Context, string, bool) error {
	return nil
}

func (f *fakeStorageNfsExports) RestoreForBucketDelete(context.Context, admin.NfsExportInfo) error {
	return nil
}

func (f *fakeStorageNfsExports) MarkBucketDeleteCleanup(string) error {
	return nil
}

func (f *fakeStorageNfsExports) ClearBucketDeleteCleanup(string) error {
	return nil
}

func (f *fakeStorageNfsExports) Get(bucket string) (admin.NfsExportInfo, bool) {
	info, ok := f.exports[bucket]
	return info, ok
}

func (f *fakeStorageNfsExports) List() []admin.NfsExportInfo {
	out := make([]admin.NfsExportInfo, 0, len(f.exports))
	for _, v := range f.exports {
		out = append(out, v)
	}
	return out
}
