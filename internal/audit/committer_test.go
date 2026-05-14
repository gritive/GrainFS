// internal/audit/committer_test.go
package audit_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// fakeBackend is an in-memory storage stub implementing audit.auditBackend.
type fakeBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newFakeBackend() *fakeBackend {
	return &fakeBackend{objects: make(map[string][]byte)}
}

func (b *fakeBackend) PutObject(_ context.Context, bucket, key string, r io.Reader, _ string) (*storage.Object, error) {
	data, _ := io.ReadAll(r)
	b.mu.Lock()
	b.objects[bucket+"/"+key] = data
	b.mu.Unlock()
	return &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func (b *fakeBackend) GetObject(_ context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	b.mu.Lock()
	data, ok := b.objects[bucket+"/"+key]
	b.mu.Unlock()
	if !ok {
		return nil, nil, fmt.Errorf("not found: %s/%s", bucket, key)
	}
	return io.NopCloser(strings.NewReader(string(data))), &storage.Object{Key: key, Size: int64(len(data))}, nil
}

func (b *fakeBackend) CreateBucket(_ context.Context, _ string) error { return nil }

// fakeCatalog is an in-memory icebergcatalog.Catalog stub.
type fakeCatalog struct {
	mu      sync.Mutex
	tables  map[string]*icebergcatalog.Table
	commits int
}

func newFakeCatalog() *fakeCatalog {
	return &fakeCatalog{tables: make(map[string]*icebergcatalog.Table)}
}

func (c *fakeCatalog) Warehouse() string { return "s3://grainfs-audit" }

func (c *fakeCatalog) CreateNamespace(_ context.Context, _ []string, _ map[string]string) error {
	return nil
}

func (c *fakeCatalog) LoadNamespace(_ context.Context, _ []string) (map[string]string, error) {
	return nil, nil
}

func (c *fakeCatalog) ListNamespaces(_ context.Context) ([][]string, error) { return nil, nil }

func (c *fakeCatalog) DeleteNamespace(_ context.Context, _ []string) error { return nil }

func (c *fakeCatalog) CreateTable(_ context.Context, ident icebergcatalog.Identifier, in icebergcatalog.CreateTableInput) (*icebergcatalog.Table, error) {
	tbl := &icebergcatalog.Table{
		Identifier:       ident,
		MetadataLocation: in.MetadataLocation,
		Metadata:         in.Metadata,
	}
	c.mu.Lock()
	c.tables[ident.Name] = tbl
	c.mu.Unlock()
	return tbl, nil
}

func (c *fakeCatalog) LoadTable(_ context.Context, ident icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	c.mu.Lock()
	tbl, ok := c.tables[ident.Name]
	c.mu.Unlock()
	if !ok {
		return nil, icebergcatalog.ErrTableNotFound
	}
	return tbl, nil
}

func (c *fakeCatalog) ListTables(_ context.Context, _ []string) ([]icebergcatalog.Identifier, error) {
	return nil, nil
}

func (c *fakeCatalog) DeleteTable(_ context.Context, _ icebergcatalog.Identifier) error { return nil }

func (c *fakeCatalog) CommitTable(_ context.Context, ident icebergcatalog.Identifier, in icebergcatalog.CommitTableInput) (*icebergcatalog.Table, error) {
	tbl := &icebergcatalog.Table{
		Identifier:       ident,
		MetadataLocation: in.NewMetadataLocation,
		Metadata:         in.Metadata,
	}
	c.mu.Lock()
	c.tables[ident.Name] = tbl
	c.commits++
	c.mu.Unlock()
	return tbl, nil
}

func TestCommitter_AppendFromFollower(t *testing.T) {
	catalog := newFakeCatalog()
	backend := newFakeBackend()
	emitter := audit.NewEmitter("leader-1")

	initMeta := fmt.Sprintf(audit.S3InitialMetadata, "test-uuid", "s3://grainfs-audit/audit/s3", time.Now().UnixMilli())
	_, err := catalog.CreateTable(context.Background(), icebergcatalog.Identifier{
		Namespace: []string{audit.Namespace},
		Name:      audit.TableS3,
	}, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-audit/metadata/s3/00000-test.metadata.json",
		Metadata:         json.RawMessage(initMeta),
	})
	require.NoError(t, err)
	backend.PutObject(context.Background(), audit.BucketName,
		"metadata/s3/00000-test.metadata.json",
		strings.NewReader(initMeta), "application/json")

	c := audit.NewCommitter(audit.CommitterConfig{
		Emitter:  emitter,
		Catalog:  catalog,
		Backend:  backend,
		IsLeader: func() bool { return true },
		NodeID:   "leader-1",
		Interval: 100 * time.Millisecond,
	})

	// Simulate follower shipping events to the leader before the flush tick.
	followerEvents := []audit.S3Event{
		{Bucket: "follower-bkt", Method: "PUT", Key: "f1", Status: 200},
		{Bucket: "follower-bkt", Method: "PUT", Key: "f2", Status: 200},
	}
	require.NoError(t, c.AppendFromFollower(context.Background(), followerEvents))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		catalog.mu.Lock()
		n := catalog.commits
		catalog.mu.Unlock()
		return n >= 1
	}, 2*time.Second, 50*time.Millisecond, "committer must commit follower events to Iceberg")

	backend.mu.Lock()
	var parquetData []byte
	for k, v := range backend.objects {
		if strings.HasSuffix(k, ".parquet") {
			parquetData = v
		}
	}
	backend.mu.Unlock()
	require.NotEmpty(t, parquetData, "committer must write a .parquet file")
	cancel()
	<-done
}

func TestCommitter_CommitsOutboxAndAcks(t *testing.T) {
	catalog := newFakeCatalog()
	backend := newFakeBackend()
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()

	initMeta := fmt.Sprintf(audit.S3InitialMetadata, "test-uuid", "s3://grainfs-audit/audit/s3", time.Now().UnixMilli())
	_, err = catalog.CreateTable(context.Background(), icebergcatalog.Identifier{
		Namespace: []string{audit.Namespace},
		Name:      audit.TableS3,
	}, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-audit/metadata/s3/00000-test.metadata.json",
		Metadata:         json.RawMessage(initMeta),
	})
	require.NoError(t, err)
	_, _ = backend.PutObject(context.Background(), audit.BucketName, "metadata/s3/00000-test.metadata.json", strings.NewReader(initMeta), "application/json")

	require.NoError(t, outbox.AppendFinalized(context.Background(), audit.S3Event{
		EventID: "evt-1", RequestID: "req-1", Ts: time.Now().UnixMicro(),
		Bucket: "b", Key: "k", Method: "PUT", Operation: "PutObject", Status: 200,
	}))

	c := audit.NewCommitter(audit.CommitterConfig{
		Outbox:   outbox,
		Catalog:  catalog,
		Backend:  backend,
		IsLeader: func() bool { return true },
		NodeID:   "leader-1",
		Interval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		pending, _ := outbox.Pending(context.Background(), 10)
		return len(pending) == 0
	}, 2*time.Second, 50*time.Millisecond, "committed events must be acked from outbox")
	cancel()
	<-done
}

func TestCommitter_FollowerShipsToLeader(t *testing.T) {
	var shipped []audit.S3Event
	var mu sync.Mutex
	emitter := audit.NewEmitter("follower-1")

	c := audit.NewCommitter(audit.CommitterConfig{
		Emitter:  emitter,
		Catalog:  nil,
		Backend:  nil,
		IsLeader: func() bool { return false },
		ShipToLeader: func(_ context.Context, events []audit.S3Event) error {
			mu.Lock()
			shipped = append(shipped, events...)
			mu.Unlock()
			return nil
		},
		NodeID:   "follower-1",
		Interval: 50 * time.Millisecond,
	})

	emitter.EmitS3(audit.S3Event{Bucket: "b", Method: "GET", Key: "k", Status: 200})
	emitter.EmitS3(audit.S3Event{Bucket: "b", Method: "PUT", Key: "k2", Status: 200})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		mu.Lock()
		n := len(shipped)
		mu.Unlock()
		return n >= 2
	}, 2*time.Second, 50*time.Millisecond, "follower must ship all events to leader")

	mu.Lock()
	methods := make(map[string]bool)
	for _, e := range shipped {
		methods[e.Method] = true
	}
	mu.Unlock()
	require.True(t, methods["GET"], "shipped events must include GET")
	require.True(t, methods["PUT"], "shipped events must include PUT")
	cancel()
	<-done
}

func TestCommitter_FollowerShipsOutboxAndAcks(t *testing.T) {
	var shipped []audit.S3Event
	var mu sync.Mutex
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()
	require.NoError(t, outbox.AppendFinalized(context.Background(), audit.S3Event{
		EventID: "evt-follower-1", Bucket: "b", Method: "PUT", Key: "k", Status: 200,
	}))

	c := audit.NewCommitter(audit.CommitterConfig{
		Outbox:   outbox,
		IsLeader: func() bool { return false },
		ShipToLeader: func(_ context.Context, events []audit.S3Event) error {
			mu.Lock()
			shipped = append(shipped, events...)
			mu.Unlock()
			return nil
		},
		NodeID:   "follower-1",
		Interval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		pending, _ := outbox.Pending(context.Background(), 10)
		mu.Lock()
		n := len(shipped)
		mu.Unlock()
		return n == 1 && len(pending) == 0
	}, 2*time.Second, 50*time.Millisecond, "follower must ack outbox only after shipping")
	cancel()
	<-done
}

func TestCommitter_AppendFromFollower_ReturnsBackpressureWhenFull(t *testing.T) {
	// followerIn channel capacity is 256. Once full, AppendFromFollower must
	// return an error so the follower keeps its durable outbox pending.
	emitter := audit.NewEmitter("leader-drop")
	c := audit.NewCommitter(audit.CommitterConfig{
		Emitter:  emitter,
		Catalog:  newFakeCatalog(),
		Backend:  newFakeBackend(),
		IsLeader: func() bool { return true },
		NodeID:   "leader-drop",
		Interval: 10 * time.Second, // long interval so Run() is not draining
	})

	for i := 0; i < 256; i++ {
		require.NoError(t, c.AppendFromFollower(context.Background(), []audit.S3Event{{Bucket: "b", Method: "GET", Key: "k", Status: 200}}))
	}
	require.ErrorIs(t, c.AppendFromFollower(context.Background(), []audit.S3Event{{Bucket: "b", Method: "GET", Key: "k", Status: 200}}), audit.ErrLeaderAuditBackpressure)
}

func TestCommitter_AppendFromFollower_PersistsToLeaderOutbox(t *testing.T) {
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()

	c := audit.NewCommitter(audit.CommitterConfig{
		Outbox:   outbox,
		IsLeader: func() bool { return true },
		NodeID:   "leader-1",
	})

	require.NoError(t, c.AppendFromFollower(context.Background(), []audit.S3Event{{
		EventID: "evt-from-follower", Bucket: "b", Method: "GET", Key: "k", Status: 200,
	}}))

	pending, err := outbox.Pending(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, "evt-from-follower", pending[0].EventID)
}

func TestCommitter_UsesEventDayForParquetPath(t *testing.T) {
	catalog := newFakeCatalog()
	backend := newFakeBackend()
	outbox, err := audit.OpenOutbox(t.TempDir())
	require.NoError(t, err)
	defer outbox.Close()

	initMeta := fmt.Sprintf(audit.S3InitialMetadata, "test-uuid", "s3://grainfs-audit/audit/s3", time.Now().UnixMilli())
	_, err = catalog.CreateTable(context.Background(), icebergcatalog.Identifier{
		Namespace: []string{audit.Namespace},
		Name:      audit.TableS3,
	}, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-audit/metadata/s3/00000-test.metadata.json",
		Metadata:         json.RawMessage(initMeta),
	})
	require.NoError(t, err)
	_, _ = backend.PutObject(context.Background(), audit.BucketName, "metadata/s3/00000-test.metadata.json", strings.NewReader(initMeta), "application/json")

	require.NoError(t, outbox.AppendFinalized(context.Background(), audit.S3Event{
		EventID: "evt-day-1", Ts: time.Date(2026, 5, 14, 23, 59, 0, 0, time.UTC).UnixMicro(),
		Bucket: "b", Key: "old", Method: "PUT", Operation: "PutObject", Status: 200,
	}))
	require.NoError(t, outbox.AppendFinalized(context.Background(), audit.S3Event{
		EventID: "evt-day-2", Ts: time.Date(2026, 5, 15, 0, 1, 0, 0, time.UTC).UnixMicro(),
		Bucket: "b", Key: "new", Method: "PUT", Operation: "PutObject", Status: 200,
	}))

	c := audit.NewCommitter(audit.CommitterConfig{
		Outbox:   outbox,
		Catalog:  catalog,
		Backend:  backend,
		IsLeader: func() bool { return true },
		NodeID:   "leader-1",
		Interval: 50 * time.Millisecond,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.Run(ctx)
	}()

	require.Eventually(t, func() bool {
		backend.mu.Lock()
		defer backend.mu.Unlock()
		var saw14, saw15 bool
		for key := range backend.objects {
			saw14 = saw14 || strings.Contains(key, "data/2026-05-14/")
			saw15 = saw15 || strings.Contains(key, "data/2026-05-15/")
		}
		return saw14 && saw15
	}, 2*time.Second, 50*time.Millisecond)
	cancel()
	<-done
}

func TestCommitter_LazyBootstrap(t *testing.T) {
	// Start with an empty catalog (no table pre-created).
	// The committer must call Bootstrap() on the first ErrTableNotFound
	// and then commit successfully.
	catalog := newFakeCatalog()
	backend := newFakeBackend()
	emitter := audit.NewEmitter("leader-lazy")

	c := audit.NewCommitter(audit.CommitterConfig{
		Emitter:  emitter,
		Catalog:  catalog,
		Backend:  backend,
		IsLeader: func() bool { return true },
		NodeID:   "leader-lazy",
		Interval: 50 * time.Millisecond,
	})

	emitter.EmitS3(audit.S3Event{Bucket: "b", Method: "PUT", Key: "k", Status: 200})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go c.Run(ctx)

	require.Eventually(t, func() bool {
		catalog.mu.Lock()
		n := catalog.commits
		catalog.mu.Unlock()
		return n >= 1
	}, 3*time.Second, 50*time.Millisecond, "lazy bootstrap must create table and commit")
}

func TestCommitter_FlushesRingToIceberg(t *testing.T) {
	catalog := newFakeCatalog()
	backend := newFakeBackend()
	emitter := audit.NewEmitter("node-1")

	initMeta := fmt.Sprintf(audit.S3InitialMetadata, "test-uuid", "s3://grainfs-audit/audit/s3", time.Now().UnixMilli())
	_, err := catalog.CreateTable(context.Background(), icebergcatalog.Identifier{
		Namespace: []string{audit.Namespace},
		Name:      audit.TableS3,
	}, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-audit/metadata/s3/00000-test.metadata.json",
		Metadata:         json.RawMessage(initMeta),
	})
	require.NoError(t, err)
	backend.PutObject(context.Background(), audit.BucketName,
		"metadata/s3/00000-test.metadata.json",
		strings.NewReader(initMeta), "application/json")

	c := audit.NewCommitter(audit.CommitterConfig{
		Emitter:  emitter,
		Catalog:  catalog,
		Backend:  backend,
		IsLeader: func() bool { return true },
		NodeID:   "node-1",
		Interval: 100 * time.Millisecond,
	})

	for i := 0; i < 5; i++ {
		emitter.EmitS3(audit.S3Event{
			Bucket: "data", Method: "PUT", Key: fmt.Sprintf("obj%d", i), Status: 200,
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go c.Run(ctx)

	require.Eventually(t, func() bool {
		catalog.mu.Lock()
		n := catalog.commits
		catalog.mu.Unlock()
		return n >= 1
	}, 2*time.Second, 50*time.Millisecond, "committer must call CommitTable")

	backend.mu.Lock()
	var hasParquet bool
	for k := range backend.objects {
		if strings.HasSuffix(k, ".parquet") {
			hasParquet = true
			require.Contains(t, k, "/data/")
			require.NotContains(t, k, "dt=", "DuckDB httpfs percent-encodes '=' and breaks S3 signature validation")
		}
	}
	backend.mu.Unlock()
	require.True(t, hasParquet, "committer must write a .parquet file to backend")
}
