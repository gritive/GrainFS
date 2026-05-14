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

func (p *fakeNfsExportProposer) ProposeUpsert(_ context.Context, bucket string, cfg nfsexport.Config) (uint64, error) {
	_, err := p.store.ApplyUpsert(bucket, cfg.ReadOnly, 1)
	return 1, err
}

func (p *fakeNfsExportProposer) ProposeDelete(_ context.Context, bucket string) (uint64, error) {
	return 1, p.store.Delete(bucket)
}

func (p *fakeNfsExportProposer) ProposeBucketDeleteCascade(_ context.Context, bucket string, _ bool) (uint64, error) {
	return 1, p.store.Delete(bucket)
}

type recordingNfsBarrier struct{ indexes []uint64 }

func (b *recordingNfsBarrier) WaitApplied(_ context.Context, index uint64) error {
	b.indexes = append(b.indexes, index)
	return nil
}

func newAdminTestDepsWithNfs(t *testing.T) (*admin.Deps, *fakeBucketOps) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: &fakeNfsExportProposer{store: store},
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

func TestAdminDeleteBucketCascadesNfsExportAfterBucketDelete(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	buckets.buckets["b1"] = true
	ctx := context.Background()
	_, err := admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)

	require.NoError(t, admin.AdminDeleteBucket(ctx, d, "b1", true))
	_, ok := d.NfsExports.Get("b1")
	require.False(t, ok)
	require.False(t, buckets.buckets["b1"])
}

func TestAdminDeleteBucketDoesNotRemoveExportWhenBucketDeleteFails(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: &fakeNfsExportProposer{store: store},
	})
	d := &admin.Deps{
		Buckets:    &fakeBucketOpsNotEmpty{},
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}
	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)

	err = admin.AdminDeleteBucket(context.Background(), d, "b1", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "conflict", ae.Code)
	_, ok := d.NfsExports.Get("b1")
	require.True(t, ok)
}

func TestAdminNfsExportAllowsMultiNodeWhenBarrierIsWired(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	barrier := &recordingNfsBarrier{}
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:            store,
		Proposer:         &fakeNfsExportProposer{store: store},
		Barrier:          barrier,
		ClusterNodeCount: func() int { return 2 },
	})
	buckets := newFakeBucketOps()
	buckets.buckets["b1"] = true
	d := &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}

	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, barrier.indexes)
}
