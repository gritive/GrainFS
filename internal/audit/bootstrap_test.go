package audit_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

// bootstrapFakeBackend is an in-memory storage stub for bootstrap tests.
type bootstrapFakeBackend struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func (b *bootstrapFakeBackend) CreateBucket(_ context.Context, _ string) error { return nil }

func (b *bootstrapFakeBackend) PutObject(_ context.Context, _, key string, r io.Reader, _ string) (*storage.Object, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	b.objects[key] = data
	b.mu.Unlock()
	return &storage.Object{Size: int64(len(data))}, nil
}

func (b *bootstrapFakeBackend) GetObject(_ context.Context, _, key string) (io.ReadCloser, *storage.Object, error) {
	b.mu.Lock()
	data, ok := b.objects[key]
	b.mu.Unlock()
	if !ok {
		return nil, nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), &storage.Object{Size: int64(len(data))}, nil
}

// bootstrapFakeCatalog is an in-memory icebergcatalog.Catalog stub.
type bootstrapFakeCatalog struct {
	mu         sync.Mutex
	tables     map[string]*icebergcatalog.Table
	nsExistsOK bool // if true, CreateNamespace returns ErrNamespaceExists
	nsErr      error
}

func newBootstrapCatalog() *bootstrapFakeCatalog {
	return &bootstrapFakeCatalog{tables: make(map[string]*icebergcatalog.Table)}
}

func (c *bootstrapFakeCatalog) Warehouse() string { return "s3://grainfs-audit/" }

func (c *bootstrapFakeCatalog) CreateNamespace(_ context.Context, _ []string, _ map[string]string) error {
	if c.nsErr != nil {
		return c.nsErr
	}
	if c.nsExistsOK {
		return icebergcatalog.ErrNamespaceExists
	}
	return nil
}

func (c *bootstrapFakeCatalog) LoadNamespace(_ context.Context, _ []string) (map[string]string, error) {
	return nil, nil
}

func (c *bootstrapFakeCatalog) ListNamespaces(_ context.Context) ([][]string, error) { return nil, nil }

func (c *bootstrapFakeCatalog) DeleteNamespace(_ context.Context, _ []string) error { return nil }

func (c *bootstrapFakeCatalog) CreateTable(_ context.Context, ident icebergcatalog.Identifier, in icebergcatalog.CreateTableInput) (*icebergcatalog.Table, error) {
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

func (c *bootstrapFakeCatalog) LoadTable(_ context.Context, ident icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	c.mu.Lock()
	tbl, ok := c.tables[ident.Name]
	c.mu.Unlock()
	if !ok {
		return nil, icebergcatalog.ErrTableNotFound
	}
	return tbl, nil
}

func (c *bootstrapFakeCatalog) ListTables(_ context.Context, _ []string) ([]icebergcatalog.Identifier, error) {
	return nil, nil
}

func (c *bootstrapFakeCatalog) DeleteTable(_ context.Context, _ icebergcatalog.Identifier) error {
	return nil
}

func (c *bootstrapFakeCatalog) CommitTable(_ context.Context, ident icebergcatalog.Identifier, in icebergcatalog.CommitTableInput) (*icebergcatalog.Table, error) {
	tbl := &icebergcatalog.Table{
		Identifier:       ident,
		MetadataLocation: in.NewMetadataLocation,
		Metadata:         in.Metadata,
	}
	c.mu.Lock()
	c.tables[ident.Name] = tbl
	c.mu.Unlock()
	return tbl, nil
}

func newBootstrapBackend() *bootstrapFakeBackend {
	return &bootstrapFakeBackend{objects: make(map[string][]byte)}
}

func TestBootstrap_CreatesTableOnFirstCall(t *testing.T) {
	cat := newBootstrapCatalog()
	backend := newBootstrapBackend()

	err := audit.Bootstrap(context.Background(), cat, backend)
	require.NoError(t, err)

	tbl, err := cat.LoadTable(context.Background(), icebergcatalog.Identifier{
		Namespace: []string{audit.Namespace}, Name: audit.TableS3,
	})
	require.NoError(t, err)
	require.NotEmpty(t, tbl.MetadataLocation)
	require.Contains(t, string(tbl.Metadata), `"format-version":2`)
}

func TestBootstrap_Idempotent(t *testing.T) {
	cat := newBootstrapCatalog()
	backend := newBootstrapBackend()

	require.NoError(t, audit.Bootstrap(context.Background(), cat, backend))
	require.NoError(t, audit.Bootstrap(context.Background(), cat, backend))
}

func TestBootstrap_NamespaceExistsIgnored(t *testing.T) {
	cat := newBootstrapCatalog()
	cat.nsExistsOK = true
	backend := newBootstrapBackend()

	err := audit.Bootstrap(context.Background(), cat, backend)
	require.NoError(t, err)
}

func TestBootstrap_NamespaceErrorPropagates(t *testing.T) {
	errFake := errors.New("catalog broken")
	cat := newBootstrapCatalog()
	cat.nsErr = errFake
	backend := newBootstrapBackend()

	err := audit.Bootstrap(context.Background(), cat, backend)
	require.ErrorIs(t, err, errFake)
}
