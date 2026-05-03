package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
)

func newStartedMetaCatalogForMigrationTest(t *testing.T, backend storage.Backend) (*cluster.MetaCatalog, *cluster.MetaRaft) {
	t.Helper()
	meta, err := cluster.NewMetaRaft(cluster.MetaRaftConfig{NodeID: "node-1", DataDir: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close() })
	require.NoError(t, meta.Bootstrap())
	require.NoError(t, meta.Start(context.Background()))
	require.Eventually(t, func() bool { return meta.Node().State() == raft.Leader }, 2*time.Second, 20*time.Millisecond)
	return cluster.NewMetaCatalog(meta, backend, "s3://grainfs-tables/warehouse"), meta
}

func newLegacyStoreForMigrationTest(t *testing.T) (*badger.DB, *icebergcatalog.Store) {
	t.Helper()
	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db, icebergcatalog.NewStore(db, "s3://grainfs-tables/warehouse")
}

func TestMigrateLegacySingletonIcebergCatalogBackfillsMissingMetadataObject(t *testing.T) {
	ctx := context.Background()
	_, legacy := newLegacyStoreForMigrationTest(t)
	require.NoError(t, legacy.CreateNamespace(ctx, []string{"analytics"}, map[string]string{"owner": "eng"}))
	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	metadata := json.RawMessage(`{"format-version":2,"location":"s3://grainfs-tables/warehouse/analytics/events"}`)
	_, err := legacy.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         metadata,
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	catalog, _ := newStartedMetaCatalogForMigrationTest(t, backend)

	require.NoError(t, migrateLegacySingletonIcebergCatalog(ctx, legacy, catalog, backend))

	tbl, err := catalog.LoadTable(ctx, ident)
	require.NoError(t, err)
	require.JSONEq(t, string(metadata), string(tbl.Metadata))
	rc, _, err := backend.GetObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00000.json")
	require.NoError(t, err)
	body, err := io.ReadAll(rc)
	rc.Close()
	require.NoError(t, err)
	require.JSONEq(t, string(metadata), string(body))
}

func TestMigrateLegacySingletonIcebergCatalogSkipsMatchingExistingPointer(t *testing.T) {
	ctx := context.Background()
	_, legacy := newLegacyStoreForMigrationTest(t)
	require.NoError(t, legacy.CreateNamespace(ctx, []string{"analytics"}, nil))
	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	metadata := json.RawMessage(`{"format-version":2}`)
	_, err := legacy.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         metadata,
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(context.Background(), "grainfs-tables"))
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader(metadata), "application/json")
	require.NoError(t, err)
	catalog, _ := newStartedMetaCatalogForMigrationTest(t, backend)
	require.NoError(t, catalog.CreateNamespace(ctx, []string{"analytics"}, nil))
	_, err = catalog.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)

	require.NoError(t, migrateLegacySingletonIcebergCatalog(ctx, legacy, catalog, backend))
	require.NoError(t, migrateLegacySingletonIcebergCatalog(ctx, legacy, catalog, backend))
}

func TestMigrateLegacySingletonIcebergCatalogFailsOnConflictingTablePointer(t *testing.T) {
	ctx := context.Background()
	_, legacy := newLegacyStoreForMigrationTest(t)
	require.NoError(t, legacy.CreateNamespace(ctx, []string{"analytics"}, nil))
	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	_, err := legacy.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         json.RawMessage(`{"format-version":2}`),
	})
	require.NoError(t, err)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(context.Background(), "grainfs-tables"))
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00001.json", bytes.NewReader([]byte(`{"format-version":2}`)), "application/json")
	require.NoError(t, err)
	catalog, _ := newStartedMetaCatalogForMigrationTest(t, backend)
	require.NoError(t, catalog.CreateNamespace(ctx, []string{"analytics"}, nil))
	_, err = catalog.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00001.json",
	})
	require.NoError(t, err)

	err = migrateLegacySingletonIcebergCatalog(ctx, legacy, catalog, backend)
	require.ErrorContains(t, err, "conflicting legacy Iceberg table pointer")
}

func TestEnsureIcebergMetadataObjectTreatsNoSuchBucketAsMissingBucket(t *testing.T) {
	backend := &migrationNoSuchBucketBackend{}

	err := ensureIcebergMetadataObject(context.Background(), backend, "s3://grainfs-tables/warehouse/ns/t/metadata/00000.json", []byte(`{"format-version":2}`))
	require.NoError(t, err)
	require.True(t, backend.created)
	require.Equal(t, "grainfs-tables", backend.bucket)
	require.Equal(t, "warehouse/ns/t/metadata/00000.json", backend.key)
	require.JSONEq(t, `{"format-version":2}`, string(backend.body))
}

type migrationNoSuchBucketBackend struct {
	created bool
	bucket  string
	key     string
	body    []byte
}

func (m *migrationNoSuchBucketBackend) CreateBucket(ctx context.Context, bucket string) error {
	m.created = true
	return nil
}
func (m *migrationNoSuchBucketBackend) HeadBucket(context.Context, string) error   { return nil }
func (m *migrationNoSuchBucketBackend) DeleteBucket(context.Context, string) error { return nil }
func (m *migrationNoSuchBucketBackend) ListBuckets(context.Context) ([]string, error) {
	return nil, nil
}
func (m *migrationNoSuchBucketBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	body, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	m.bucket = bucket
	m.key = key
	m.body = body
	return &storage.Object{Key: key, Size: int64(len(body)), ContentType: contentType}, nil
}
func (m *migrationNoSuchBucketBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	return nil, nil, storage.ErrNoSuchBucket
}
func (m *migrationNoSuchBucketBackend) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	return nil, storage.ErrObjectNotFound
}
func (m *migrationNoSuchBucketBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	return nil
}
func (m *migrationNoSuchBucketBackend) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	return nil, nil
}
func (m *migrationNoSuchBucketBackend) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	return nil
}
func (m *migrationNoSuchBucketBackend) CreateMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	return nil, fmt.Errorf("unused")
}
func (m *migrationNoSuchBucketBackend) UploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader) (*storage.Part, error) {
	return nil, fmt.Errorf("unused")
}
func (m *migrationNoSuchBucketBackend) CompleteMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	return nil, fmt.Errorf("unused")
}
func (m *migrationNoSuchBucketBackend) AbortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	return fmt.Errorf("unused")
}
