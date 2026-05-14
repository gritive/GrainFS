package admin_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/server/admin"
)

type fakeNfsExportProposer struct {
	store      *nfsexport.Store
	createErr  error
	cascadeErr error
	cascades   int
}

func (p *fakeNfsExportProposer) ProposeCreate(_ context.Context, bucket string, cfg nfsexport.Config) (uint64, error) {
	if p.createErr != nil {
		return 0, p.createErr
	}
	_, err := p.store.ApplyCreate(bucket, cfg.ReadOnly, 1)
	return 1, err
}

func (p *fakeNfsExportProposer) ProposeUpsert(_ context.Context, bucket string, cfg nfsexport.Config) (uint64, error) {
	if cfg.FsidMinor != 0 || cfg.Generation != 0 {
		return 1, p.store.Put(bucket, cfg)
	}
	_, err := p.store.ApplyUpsert(bucket, cfg.ReadOnly, 1)
	return 1, err
}

func (p *fakeNfsExportProposer) ProposeDelete(_ context.Context, bucket string) (uint64, error) {
	return 1, p.store.Delete(bucket)
}

func (p *fakeNfsExportProposer) ProposeBucketDeleteCascade(_ context.Context, bucket string, _ bool) (uint64, error) {
	if p.cascadeErr != nil {
		return 0, p.cascadeErr
	}
	p.cascades++
	return 1, p.store.Delete(bucket)
}

type recordingNfsBarrier struct {
	indexes []uint64
	err     error
}

func (b *recordingNfsBarrier) WaitApplied(_ context.Context, index uint64) error {
	b.indexes = append(b.indexes, index)
	return b.err
}

type fakeNFSDiag struct {
	lookups []nfs4server.LookupRecord
	clients []string
}

func (f *fakeNFSDiag) RecentLookups(_ string, _ time.Duration) []nfs4server.LookupRecord {
	return f.lookups
}

func (f *fakeNFSDiag) ActiveMountClients(_ string) []string {
	return f.clients
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

func startAdminNfsHTTP(t *testing.T) (*http.Client, string) {
	t.Helper()
	dir := shortTempDir(t)
	sock := filepath.Join(dir, "admin.sock")
	deps, _ := newAdminTestDepsWithNfs(t)
	srv, err := admin.Start(admin.Config{SocketPath: sock, Deps: deps})
	require.NoError(t, err)
	t.Cleanup(func() { _ = srv.Stop(context.Background()) })
	return unixHTTPClient(sock), "http://unix"
}

func TestExportAdd_BucketNotFound_Returns404(t *testing.T) {
	client, baseURL := startAdminNfsHTTP(t)
	resp, err := client.Post(baseURL+"/v1/nfs/exports", "application/json", bytes.NewBufferString(`{"bucket":"missing"}`))
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode, "must be 404 NOT FOUND, not 500")
	var e adminapi.Error
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&e))
	require.Equal(t, "bucket_not_found", e.Code)
	require.NotEmpty(t, e.Help)
	require.NotEmpty(t, e.DocsURL)
}

func TestExportGet_NotFound_Returns404(t *testing.T) {
	client, baseURL := startAdminNfsHTTP(t)
	resp, err := client.Get(baseURL + "/v1/nfs/exports/missing")
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode, "must be 404 NOT FOUND, not 500")
	var e adminapi.Error
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&e))
	require.Equal(t, "export_not_found", e.Code)
}

func TestAdminNfsExportDebug_Registered(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	buckets.buckets["my-data"] = true
	buckets.counts["my-data"] = 1234
	d.NodeID = "node-a"
	d.NFSDiag = &fakeNFSDiag{
		lookups: []nfs4server.LookupRecord{{
			Client: "10.0.0.5:2049",
			Bucket: "my-data",
			Result: "ok",
			At:     time.Unix(100, 0),
		}},
		clients: []string{"10.0.0.5:2049"},
	}
	_, err := admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "my-data"})
	require.NoError(t, err)

	resp, err := admin.AdminNfsExportDebug(context.Background(), d, "my-data")
	require.NoError(t, err)
	require.True(t, resp.Registered)
	require.True(t, resp.BackendBucket.Exists)
	require.EqualValues(t, 1234, resp.BackendBucket.ObjectCount)
	require.Equal(t, []string{"10.0.0.5:2049"}, resp.ActiveMountClients)
	require.Len(t, resp.RecentLookups, 1)
}

func TestAdminNfsExportDebugRejectsInternalBucket(t *testing.T) {
	d, _ := newAdminTestDepsWithNfs(t)
	_, err := admin.AdminNfsExportDebug(context.Background(), d, "__grainfs_internal")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "forbidden", ae.Code)
}

type nfsDebugBucketHeadError struct{ *fakeBucketOps }

func (f nfsDebugBucketHeadError) HeadBucket(context.Context, string) error {
	return errors.New("badger unavailable")
}

type nfsDebugBucketCountError struct{ *fakeBucketOps }

func (f nfsDebugBucketCountError) CountObjects(context.Context, string) (int64, error) {
	return 0, errors.New("badger unavailable")
}

func TestAdminNfsExportDebugSurfacesBackendErrors(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	buckets.buckets["my-data"] = true

	d.Buckets = nfsDebugBucketHeadError{fakeBucketOps: buckets}
	_, err := admin.AdminNfsExportDebug(context.Background(), d, "my-data")
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "internal", ae.Code)
	require.Contains(t, ae.Message, "head bucket")

	d.Buckets = nfsDebugBucketCountError{fakeBucketOps: buckets}
	_, err = admin.AdminNfsExportDebug(context.Background(), d, "my-data")
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "internal", ae.Code)
	require.Contains(t, ae.Message, "count objects")
}

func TestAdminNfsExportUpsertPropagationTimeout(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	buckets := newFakeBucketOps()
	buckets.buckets["b1"] = true
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: &fakeNfsExportProposer{store: store},
		Barrier:  &recordingNfsBarrier{err: context.DeadlineExceeded},
	})
	d := &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}

	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "export_propagation_timeout", ae.Code)
}

func TestAdminNfsExportUpsertCapabilityRejectIsUnsupported(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	buckets := newFakeBucketOps()
	buckets.buckets["b1"] = true
	plan := compat.GatePlan{
		Capability: compat.CapabilityNfsExportCreateV1,
		Scope:      compat.ScopeMetaRaft,
		Severity:   compat.SeverityHard,
		Operation:  compat.OperationNfsExportCreate,
		Unknown:    []compat.NodeID{"node-b"},
	}
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: &fakeNfsExportProposer{store: store, createErr: compat.Reject(plan)},
	})
	d := &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}

	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "unsupported", ae.Code)
	require.Contains(t, ae.Message, compat.CapabilityNfsExportCreateV1)
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

func TestAdminNfsExportAddRejectsExistingExport(t *testing.T) {
	d, buckets := newAdminTestDepsWithNfs(t)
	buckets.buckets["b1"] = true
	ctx := context.Background()

	created, err := admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)
	_, err = admin.AdminNfsExportUpsert(ctx, d, admin.NfsExportUpsertReq{Bucket: "b1", ReadOnly: true})

	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "conflict", ae.Code)
	require.Contains(t, ae.Message, "already registered")

	got, ok := d.NfsExports.Get("b1")
	require.True(t, ok)
	require.Equal(t, created, got)
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

func TestAdminDeleteBucketCleansExportWhenBucketAlreadyMissing(t *testing.T) {
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
	d := &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}
	ctx := context.Background()
	_, err = store.ApplyUpsert("b1", false, 1)
	require.NoError(t, err)

	err = admin.AdminDeleteBucket(ctx, d, "b1", true)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "not_found", ae.Code)
	_, ok := d.NfsExports.Get("b1")
	require.False(t, ok)
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
	before, ok := d.NfsExports.Get("b1")
	require.True(t, ok)

	err = admin.AdminDeleteBucket(context.Background(), d, "b1", false)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "conflict", ae.Code)
	restored, ok := d.NfsExports.Get("b1")
	require.True(t, ok)
	require.Equal(t, before, restored)
	pending, err := svc.PendingBucketDeleteCleanups()
	require.NoError(t, err)
	require.Empty(t, pending)
}

type fakeBucketOpsConcurrentExportReadd struct {
	*fakeBucketOps
	readd func()
}

func (f *fakeBucketOpsConcurrentExportReadd) ForceDeleteBucket(ctx context.Context, bucket string) error {
	err := f.fakeBucketOps.ForceDeleteBucket(ctx, bucket)
	if err == nil && f.readd != nil {
		f.readd()
	}
	return err
}

func TestAdminDeleteBucketClearsConcurrentExportReadd(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	proposer := &fakeNfsExportProposer{store: store}
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: proposer,
	})
	buckets := &fakeBucketOpsConcurrentExportReadd{
		fakeBucketOps: newFakeBucketOps(),
		readd: func() {
			_, err := store.ApplyUpsert("b1", false, 1)
			require.NoError(t, err)
		},
	}
	buckets.buckets["b1"] = true
	d := &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}

	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)
	require.NoError(t, admin.AdminDeleteBucket(context.Background(), d, "b1", true))
	_, ok := d.NfsExports.Get("b1")
	require.False(t, ok)
	require.Equal(t, 1, proposer.cascades)
}

func TestAdminDeleteBucketClearsExportAddedDuringDelete(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	proposer := &fakeNfsExportProposer{store: store}
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: proposer,
	})
	buckets := &fakeBucketOpsConcurrentExportReadd{
		fakeBucketOps: newFakeBucketOps(),
		readd: func() {
			_, err := store.ApplyUpsert("b1", false, 1)
			require.NoError(t, err)
		},
	}
	buckets.buckets["b1"] = true
	d := &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}

	require.NoError(t, admin.AdminDeleteBucket(context.Background(), d, "b1", true))
	_, ok := d.NfsExports.Get("b1")
	require.False(t, ok)
	require.Equal(t, 1, proposer.cascades)
	pending, err := svc.PendingBucketDeleteCleanups()
	require.NoError(t, err)
	require.Empty(t, pending)
}

func TestAdminDeleteBucketLeavesCleanupMarkerWhenCascadeFailsAfterBucketDelete(t *testing.T) {
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	proposer := &fakeNfsExportProposer{store: store}
	svc := nfsexport.NewExportService(nfsexport.ServiceConfig{
		Store:    store,
		Proposer: proposer,
	})
	buckets := newFakeBucketOps()
	buckets.buckets["b1"] = true
	d := &admin.Deps{
		Buckets:    buckets,
		NfsExports: &admin.NfsExportServiceAdapter{Svc: svc},
	}
	_, err = admin.AdminNfsExportUpsert(context.Background(), d, admin.NfsExportUpsertReq{Bucket: "b1"})
	require.NoError(t, err)
	proposer.cascadeErr = assertAnError{}

	err = admin.AdminDeleteBucket(context.Background(), d, "b1", true)
	var ae *adminapi.Error
	require.ErrorAs(t, err, &ae)
	require.Equal(t, "internal", ae.Code)
	require.False(t, buckets.buckets["b1"])
	_, ok := d.NfsExports.Get("b1")
	require.True(t, ok)
	pending, err := svc.PendingBucketDeleteCleanups()
	require.NoError(t, err)
	require.Equal(t, []string{"b1"}, pending)
}

type assertAnError struct{}

func (assertAnError) Error() string { return "raft unavailable" }

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
