package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// --- helpers ---

// jwtAuthnServer starts a test server with:
//   - a fresh KeySet (keys generated)
//   - no policyAuthorizer (gate disabled → JWT verify is the only gate)
//   - optional anonCfg to override iam.anon-enabled
//   - optional policyAuthorizer for action-deny tests
func setupJWTAuthnServer(t *testing.T, opts ...Option) (base string, keys *iamjwt.KeySet) {
	t.Helper()
	ks := iamjwt.NewKeySet()
	_, err := ks.GenerateCurrent()
	require.NoError(t, err)

	allOpts := append([]Option{WithJWTKeySet(ks)}, opts...)
	base = setupTestServerWithOptions(t, allOpts...)
	return base, ks
}

// mintBearer mints a valid JWT for the given warehouse using ks.
func mintBearer(t *testing.T, ks *iamjwt.KeySet, warehouse string) string {
	t.Helper()
	tok, err := ks.Mint(iamjwt.Claims{Sub: "sa-test", Warehouse: warehouse, TTL: time.Hour})
	require.NoError(t, err)
	return tok
}

// decodeAuthnErrorBody decodes the Iceberg error envelope {"error":{"message","type","code"}}.
func decodeAuthnErrorBody(t *testing.T, resp *http.Response) (string, int) {
	t.Helper()
	var env struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    int    `json:"code"`
		} `json:"error"`
	}
	raw, _ := io.ReadAll(resp.Body)
	require.NoError(t, json.Unmarshal(raw, &env), "body: %s", raw)
	return env.Error.Message, env.Error.Code
}

// waitForPort loops until addr is connectable or test times out.
func waitForPort(t *testing.T, addr string) {
	t.Helper()
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// fakeEmptyPolicyStore implements policy.Store with zero data — all SAs get
// empty policy sets, resulting in implicit Deny from policy.Evaluate.
type fakeEmptyPolicyStore struct{}

func (fakeEmptyPolicyStore) SAPolicies(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (fakeEmptyPolicyStore) SAGroups(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (fakeEmptyPolicyStore) GroupPolicies(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (fakeEmptyPolicyStore) PolicyDoc(_ context.Context, _ string) (*policy.Document, error) {
	return nil, nil
}
func (fakeEmptyPolicyStore) BucketPolicy(_ context.Context, _ string) (*policy.Document, error) {
	return nil, nil
}

// denyAllAuthorizer builds an s3auth.Authorizer backed by empty policy
// resolver + anon-disabled config → every request gets implicit Deny.
func denyAllAuthorizer(t *testing.T) *s3auth.Authorizer {
	t.Helper()
	res := policy.NewResolver(fakeEmptyPolicyStore{}, 0)
	cfg := anonConfigReader{"iam.anon-enabled": false}
	return s3auth.NewAuthorizer(res, cfg)
}

// --- tests ---

// TestIcebergAuthn_MissingToken_401: anon=false, no bearer header → 401.
// Uses a SigV4-enabled server so the SigV4 gate (existing authMiddleware)
// produces the 401 when no credentials are present at all.
func TestIcebergAuthn_MissingToken_401(t *testing.T) {
	// WithAuth wires the SigV4 verifier → requests without auth → 401.
	base, _ := setupJWTAuthnServer(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "testkey", SecretKey: "testsecret"}}),
	)

	resp, err := http.Get(base + "/iceberg/v1/config?warehouse=wh")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
}

// TestIcebergAuthn_InvalidSignature_401: anon=false, fake Bearer token → 401.
func TestIcebergAuthn_InvalidSignature_401(t *testing.T) {
	base, _ := setupJWTAuthnServer(t)

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=wh", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer fake.fake.fake")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")
	msg, _ := decodeAuthnErrorBody(t, resp)
	assert.NotEmpty(t, msg)
}

// TestIcebergAuthn_AlgNoneInBearer_401: alg=none token must be rejected.
// T34 rejects it in Verify(), middleware must propagate → 401.
func TestIcebergAuthn_AlgNoneInBearer_401(t *testing.T) {
	base, _ := setupJWTAuthnServer(t)

	// Build a hand-crafted alg:none token (no signature).
	hdr := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`))
	payload := base64.RawURLEncoding.EncodeToString([]byte(
		fmt.Sprintf(`{"sub":"attacker","warehouse":"wh","iss":"grainfs","iat":%d,"exp":%d}`,
			time.Now().Unix(), time.Now().Add(time.Hour).Unix()),
	))
	algNoneToken := hdr + "." + payload + "." // empty signature

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=wh", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+algNoneToken)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)
}

// TestIcebergAuthn_WrongWarehouseClaim_403: token minted for "wh-A", request
// asks for ?warehouse=wh-B → 403.
func TestIcebergAuthn_WrongWarehouseClaim_403(t *testing.T) {
	base, ks := setupJWTAuthnServer(t)

	tok := mintBearer(t, ks, "wh-A")
	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=wh-B", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	msg, _ := decodeAuthnErrorBody(t, resp)
	assert.Contains(t, msg, "warehouse claim mismatch")
}

// TestIcebergAuthn_ActionDenied_403: valid JWT + denyAll authorizer → 403.
func TestIcebergAuthn_ActionDenied_403(t *testing.T) {
	authz := denyAllAuthorizer(t)
	base, ks := setupJWTAuthnServer(t, WithPolicyAuthorizer(authz))

	tok := mintBearer(t, ks, "mywh")
	// POST to /v1/namespaces/:namespace/tables/:table → CommitTable action.
	req, err := http.NewRequest(http.MethodPost,
		base+"/iceberg/v1/namespaces/ns1/tables/t1",
		strings.NewReader("{}"),
	)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	msg, _ := decodeAuthnErrorBody(t, resp)
	assert.Contains(t, msg, "policy denied")
}

// TestIcebergAuthn_AnonPhase0_NoBearerNeeded: anon=true → bearer gate skipped
// even when an INVALID bearer token is present.
// A request with a garbage "Bearer bad.token" header must NOT be rejected with
// 401 when iam.anon-enabled=true — the anon short-circuit fires first.
func TestIcebergAuthn_AnonPhase0_NoBearerNeeded(t *testing.T) {
	anonCfg := anonConfigReader{"iam.anon-enabled": true}

	// Build a server with no SigV4 gate (no WithAuth) so the only auth layer is
	// the bearer/anon gate under test.
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	ks := iamjwt.NewKeySet()
	_, err = ks.GenerateCurrent()
	require.NoError(t, err)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend,
		WithJWTKeySet(ks),
		WithBearerConfig(anonCfg),
	)
	go srv.Run() //nolint:errcheck
	waitForPort(t, addr)
	anonBase := "http://" + addr

	// Send an INVALID bearer token — icebergAuthnCheck would reject it 401,
	// but anon short-circuit in icebergGuarded must fire before Verify() is called.
	req, err := http.NewRequest(http.MethodGet, anonBase+"/iceberg/v1/config?warehouse=default", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer this.is.not.a.valid.jwt")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	// anon-enabled must short-circuit before JWT verification → must NOT be 401.
	assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode, "anon-enabled must skip bearer requirement even for invalid tokens")
}

// TestIcebergAuthn_ValidBearer_Passes: happy path; valid JWT for correct
// warehouse → not 401/403. Claims should be stashed in context (verified
// indirectly via non-error response).
func TestIcebergAuthn_ValidBearer_Passes(t *testing.T) {
	// Build a server with no SigV4 gate (no WithAuth) so only bearer matters.
	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	ks := iamjwt.NewKeySet()
	_, err = ks.GenerateCurrent()
	require.NoError(t, err)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithJWTKeySet(ks))
	go srv.Run() //nolint:errcheck
	waitForPort(t, addr)
	happyBase := "http://" + addr

	tok := mintBearer(t, ks, "warehouse")
	req, err := http.NewRequest(http.MethodGet, happyBase+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode, "valid bearer must pass auth")
	assert.NotEqual(t, http.StatusForbidden, resp.StatusCode, "valid bearer must pass auth")
}
