package iceberg

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/iampb"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

// testAKKey is a package-local context key used to thread the access key into
// the handler under test. Core owns the production access-key context key
// (auth_context.go) and the matched WithAccessKey/AccessKeyFromContext pair —
// neither is reachable here (would require importing server, breaking the
// one-way edge). The identity of the production key is covered by the
// black-box scheme/secret tests that stay in package server; these white-box
// tests only need a way to inject the caller access key for the lookup path.
type testAKKey struct{}

func withTestAccessKey(ctx context.Context, ak string) context.Context {
	return context.WithValue(ctx, testAKKey{}, ak)
}

func testAccessKeyFromContext(ctx context.Context) string {
	v, _ := ctx.Value(testAKKey{}).(string)
	return v
}

// iamFixture mints a minimal IAM store with SAs + keys, mirroring the core
// iamTestHelper just enough for the cred-override lookup path.
type iamFixture struct {
	store   *iam.Store
	applier *iam.Applier
	enc     storage.DataEncryptor
}

func newIAMFixture(t *testing.T) *iamFixture {
	t.Helper()
	clusterID := bytes.Repeat([]byte{0xcd}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0xab}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	de := storage.NewDEKKeeperAdapter(keeper, clusterID)
	store := iam.NewStore()
	ap := iam.NewApplier(store, de)
	return &iamFixture{store: store, applier: ap, enc: de}
}

func (f *iamFixture) applySACreate(t *testing.T, saID string) {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	idOff := b.CreateString(saID)
	nameOff := b.CreateString(saID)
	descOff := b.CreateString("")
	cbOff := b.CreateString("")
	iampb.SACreatePayloadStart(b)
	iampb.SACreatePayloadAddSaId(b, idOff)
	iampb.SACreatePayloadAddName(b, nameOff)
	iampb.SACreatePayloadAddDescription(b, descOff)
	iampb.SACreatePayloadAddCreatedAtUnixNs(b, time.Now().UnixNano())
	iampb.SACreatePayloadAddCreatedBy(b, cbOff)
	end := iampb.SACreatePayloadEnd(b)
	b.Finish(end)
	require.NoError(t, f.applier.ApplySACreate(b.FinishedBytes()))
}

func (f *iamFixture) applyKeyCreate(t *testing.T, ak, saID, secret string) {
	t.Helper()
	wrapped, _, err := iam.WrapSecret(f.enc, saID, ak, secret)
	require.NoError(t, err)

	b := flatbuffers.NewBuilder(128)
	akOff := b.CreateString(ak)
	saOff := b.CreateString(saID)
	encOff := b.CreateByteVector(wrapped)
	iampb.KeyCreatePayloadStart(b)
	iampb.KeyCreatePayloadAddAccessKey(b, akOff)
	iampb.KeyCreatePayloadAddSecretKeyEnc(b, encOff)
	iampb.KeyCreatePayloadAddSaId(b, saOff)
	iampb.KeyCreatePayloadAddCreatedAtUnixNs(b, time.Now().UnixNano())
	end := iampb.KeyCreatePayloadEnd(b)
	b.Finish(end)
	require.NoError(t, f.applier.ApplyKeyCreate(b.FinishedBytes()))
}

// fakeIcebergCatalog is a minimal Catalog stub for config-handler tests. Mirrors
// the core test stub (which stays in package server for the black-box suite).
type fakeIcebergCatalog struct {
	warehouse string
}

func (f fakeIcebergCatalog) Warehouse() string { return f.warehouse }
func (f fakeIcebergCatalog) CreateNamespace(context.Context, string, []string, map[string]string) error {
	return icebergcatalog.ErrNamespaceExists
}
func (f fakeIcebergCatalog) LoadNamespace(context.Context, string, []string) (map[string]string, error) {
	return nil, icebergcatalog.ErrNamespaceNotFound
}
func (f fakeIcebergCatalog) ListNamespaces(context.Context, string) ([][]string, error) {
	return nil, nil
}
func (f fakeIcebergCatalog) DeleteNamespace(context.Context, string, []string) error {
	return icebergcatalog.ErrNamespaceNotFound
}
func (f fakeIcebergCatalog) CreateTable(context.Context, string, icebergcatalog.Identifier, icebergcatalog.CreateTableInput) (*icebergcatalog.Table, error) {
	return nil, icebergcatalog.ErrTableExists
}
func (f fakeIcebergCatalog) LoadTable(context.Context, string, icebergcatalog.Identifier) (*icebergcatalog.Table, error) {
	return nil, icebergcatalog.ErrTableNotFound
}
func (f fakeIcebergCatalog) ListTables(context.Context, string, []string) ([]icebergcatalog.Identifier, error) {
	return nil, nil
}
func (f fakeIcebergCatalog) DeleteTable(context.Context, string, icebergcatalog.Identifier) error {
	return icebergcatalog.ErrTableNotFound
}
func (f fakeIcebergCatalog) CommitTable(context.Context, string, icebergcatalog.Identifier, icebergcatalog.CommitTableInput) (*icebergcatalog.Table, error) {
	return nil, icebergcatalog.ErrCommitFailed
}

// newCredTestHandler builds a Handler with only the deps the cred-override path
// exercises. PolicyAuthorizer is intentionally nil so icebergS3CredOverrides
// hits the fail-open branch (matching the original &Server{iamStore: ...}, which
// also left policyAuthorizer nil).
func newCredTestHandler(store *iam.Store, catalog icebergcatalog.Catalog) *Handler {
	return NewHandler(Deps{
		IAMStore:            store,
		Catalog:             catalog,
		PolicyAuthorizer:    nil,
		AccessKey:           testAccessKeyFromContext,
		AuditSinkConfigured: func() bool { return false },
		FeatureAvailable:    func() bool { return catalog != nil },
	})
}

// TestIcebergS3CredOverrides_CallerIdentity verifies that /v1/config publishes
// the caller's *own* access/secret pair — not some other SA's keys (privilege
// amplification prevention). Legacy grant-check removed in §2; credential
// forwarding is best-effort for the key that authenticated the request.
func TestIcebergS3CredOverrides_CallerIdentity(t *testing.T) {
	f := newIAMFixture(t)

	f.applySACreate(t, "sa-alpha")
	f.applyKeyCreate(t, "ak-alpha", "sa-alpha", "sk-alpha")

	f.applySACreate(t, "sa-beta")
	f.applyKeyCreate(t, "ak-beta", "sa-beta", "sk-beta")

	h := newCredTestHandler(f.store, nil)
	warehouse := "s3://grainfs-tables/warehouse"

	t.Run("alpha caller gets own creds", func(t *testing.T) {
		ctx := withTestAccessKey(context.Background(), "ak-alpha")
		got := h.icebergS3CredOverrides(ctx, warehouse)
		require.Equal(t, "ak-alpha", got["s3.access-key-id"],
			"caller must receive their own ak, not another SA's")
		require.Equal(t, "sk-alpha", got["s3.secret-access-key"])
		require.Equal(t, "true", got["s3.path-style-access"])
	})

	t.Run("beta caller gets own creds", func(t *testing.T) {
		ctx := withTestAccessKey(context.Background(), "ak-beta")
		got := h.icebergS3CredOverrides(ctx, warehouse)
		require.Equal(t, "ak-beta", got["s3.access-key-id"])
		require.Equal(t, "sk-beta", got["s3.secret-access-key"])
	})

	t.Run("no caller identity means empty overrides", func(t *testing.T) {
		got := h.icebergS3CredOverrides(context.Background(), warehouse)
		require.Empty(t, got)
	})

	t.Run("unknown access key means empty overrides", func(t *testing.T) {
		ctx := withTestAccessKey(context.Background(), "ak-never-existed")
		got := h.icebergS3CredOverrides(ctx, warehouse)
		require.Empty(t, got)
	})

	t.Run("malformed warehouse means empty overrides", func(t *testing.T) {
		ctx := withTestAccessKey(context.Background(), "ak-alpha")
		got := h.icebergS3CredOverrides(ctx, "not-an-s3-url")
		require.Empty(t, got)
	})
}

func TestIcebergConfigHandler_HTTPSPublishesCallerS3Secrets(t *testing.T) {
	f := newIAMFixture(t)
	f.applySACreate(t, "sa-bench")
	f.applyKeyCreate(t, "AK-bench", "sa-bench", "SK-bench")
	h := newCredTestHandler(f.store, fakeIcebergCatalog{warehouse: "s3://grainfs-tables/warehouse"})
	ctx := withTestAccessKey(context.Background(), "AK-bench")
	c := app.NewContext(0)
	c.Request.SetRequestURI("/iceberg/v1/config?warehouse=warehouse")
	c.Request.SetHost("grainfs.example")
	c.Request.URI().SetScheme("https")

	h.icebergConfig(ctx, c)

	require.Equal(t, http.StatusOK, c.Response.StatusCode())
	var got struct {
		Defaults  map[string]string `json:"defaults"`
		Overrides map[string]string `json:"overrides"`
	}
	require.NoError(t, json.Unmarshal(c.Response.Body(), &got))
	require.Equal(t, "AK-bench", got.Overrides["s3.access-key-id"])
	require.Equal(t, "SK-bench", got.Overrides["s3.secret-access-key"])
	require.Equal(t, "true", got.Overrides["s3.path-style-access"])
	require.Equal(t, "https://grainfs.example", got.Overrides["s3.endpoint"])
}
