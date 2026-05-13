package admin_test

import (
	"context"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/server/admin"
)

type fakeNfsExportProposer struct {
	store *nfsexport.Store
}

func (p *fakeNfsExportProposer) ProposeUpsert(_ context.Context, bucket string, cfg nfsexport.Config) error {
	return p.store.Put(bucket, cfg)
}

func (p *fakeNfsExportProposer) ProposeDelete(_ context.Context, bucket string) error {
	return p.store.Delete(bucket)
}

func newAdminTestDepsWithNfs(t *testing.T) (*admin.Deps, *fakeBucketOps) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:     store,
		Proposer:  &fakeNfsExportProposer{store: store},
		FsidMajor: 1,
	})
	buckets := newFakeBucketOps()
	return &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}, buckets
}

func TestAdminNfsExportUpsertValidation(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	ctx := context.Background()

	_, err := admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{})
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "invalid", ae.Code)
	require.Equal(t, "bucket", ae.Param)

	_, err = admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "__grainfs_internal"})
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "forbidden", ae.Code)

	_, err = admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "missing"})
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "bucket_not_found", ae.Code)
	require.NotEmpty(t, ae.Help)
	require.NotEmpty(t, ae.DocsURL)

	buckets.buckets["b1"] = true
	info, err := admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)
	require.Equal(t, uint64(1), info.Generation)
	require.NotZero(t, info.FsidMinor)
}

func TestAdminNfsExportCRUD(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	buckets.buckets["b1"] = true
	ctx := context.Background()

	created, err := admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)
	updated, err := admin.AdminNfsExportUpdate(ctx, d, "b1", admin.NfsExportUpsertReq{ReadOnly: true})
	require.NoError(t, err)
	require.Equal(t, created.FsidMinor, updated.FsidMinor)
	require.Equal(t, uint64(2), updated.Generation)
	require.True(t, updated.ReadOnly)

	got, err := admin.AdminNfsExportGet(ctx, d, "b1")
	require.NoError(t, err)
	require.Equal(t, updated, got)
	list, err := admin.AdminNfsExportList(ctx, d)
	require.NoError(t, err)
	require.Equal(t, []admin.NfsExportInfo{updated}, list.Exports)

	require.NoError(t, admin.AdminNfsExportDelete(ctx, d, "b1"))
	require.NoError(t, admin.AdminNfsExportDelete(ctx, d, "b1"))
	_, err = admin.AdminNfsExportGet(ctx, d, "b1")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "export_not_found", ae.Code)
}

func TestAdminNfsExportUpdateMissing(t *testing.T) {
	d, _ := newAdminTestDepsWithNfs(t)
	_, err := admin.AdminNfsExportUpdate(context.Background(), d, "missing", admin.NfsExportUpsertReq{})
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "export_not_found", ae.Code)
}

func TestAdminDeleteBucketCascadesNfsExport(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	buckets.buckets["b1"] = true
	ctx := context.Background()
	_, err := admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)

	require.NoError(t, admin.AdminDeleteBucket(ctx, d, "b1", true))
	_, ok := d.NfsExports.Get("b1")
	require.False(t, ok)
}
