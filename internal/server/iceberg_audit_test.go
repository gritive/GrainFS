package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/audit"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/s3auth"
	"github.com/gritive/GrainFS/internal/storage"
)

// allowPolicyStore returns Allow for sa-test via a single policy document named
// "test-policy-id" with Sid "AllowAll". Used to verify MatchedPolicyID / MatchedSID
// are propagated into the audit row.
type allowPolicyStore struct{}

func (allowPolicyStore) SAPolicies(_ context.Context, saID string) ([]string, error) {
	if saID == "sa-test" {
		return []string{"test-policy-id"}, nil
	}
	return nil, nil
}
func (allowPolicyStore) SAGroups(_ context.Context, _ string) ([]string, error) { return nil, nil }
func (allowPolicyStore) GroupPolicies(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}
func (allowPolicyStore) PolicyDoc(_ context.Context, name string) (*policy.Document, error) {
	if name == "test-policy-id" {
		return &policy.Document{
			Statement: []policy.Statement{{
				Sid:      "AllowAll",
				Effect:   policy.EffectAllow,
				Action:   policy.StringOrSlice{"iceberg:GetCatalogConfig", "iceberg:CommitTable"},
				Resource: policy.StringOrSlice{"*"},
			}},
		}, nil
	}
	return nil, nil
}
func (allowPolicyStore) BucketPolicy(_ context.Context, _ string) (*policy.Document, error) {
	return nil, nil
}

// allowAuthorizer builds an s3auth.Authorizer backed by allowPolicyStore.
func allowAuthorizer(t *testing.T) *s3auth.Authorizer {
	t.Helper()
	res := policy.NewResolver(allowPolicyStore{}, 0)
	cfg := anonConfigReader{"iam.anon-enabled": false}
	return s3auth.NewAuthorizer(res, cfg)
}

// drainAuditEvents drains all audit events emitted by e since last call.
func drainAuditEvents(e *audit.Emitter) []audit.S3Event {
	return e.Ring().DrainInto(nil)
}

// findIcebergAuditEvent returns the first audit.s3 event whose Operation matches op.
func findIcebergAuditEvent(events []audit.S3Event, op string) (audit.S3Event, bool) {
	for _, ev := range events {
		if ev.Operation == op {
			return ev, true
		}
	}
	return audit.S3Event{}, false
}

// TestIcebergGuarded_EmitsAllowAuditRow checks that a successful bearer request
// produces an allow audit.s3 row with matched_policy_id and matched_sid populated
// when a policy authorizer is configured.
func TestIcebergGuarded_EmitsAllowAuditRow(t *testing.T) {
	emitter := audit.NewEmitterWithRingCapacity("test-node", 64)
	authz := allowAuthorizer(t)

	base, ks := setupJWTAuthnServer(t,
		WithPolicyAuthorizer(authz),
		WithAuditEmitter(emitter),
	)

	tok := mintBearer(t, ks, "warehouse")
	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=warehouse", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)
	assert.NotEqual(t, http.StatusForbidden, resp.StatusCode)

	// Allow a short moment for the async emit path.
	time.Sleep(20 * time.Millisecond)

	events := drainAuditEvents(emitter)
	ev, found := findIcebergAuditEvent(events, "iceberg:GetCatalogConfig")
	require.True(t, found, "expected audit event for iceberg:GetCatalogConfig; got events: %+v", events)
	assert.Equal(t, "allow", ev.AuthStatus, "AuthStatus should be allow")
	assert.Equal(t, "sa-test", ev.SAID, "SAID should be the JWT sub claim")
	assert.Equal(t, "warehouse", ev.Bucket, "Bucket should be the warehouse")
	// §6 policy-decision columns must be populated from EvalResult.
	assert.Equal(t, "test-policy-id", ev.MatchedPolicyID, "MatchedPolicyID must be set from EvalResult.MatchedPolicy")
	assert.Equal(t, "AllowAll", ev.MatchedSID, "MatchedSID must be set from EvalResult.MatchedSid")
	assert.NotZero(t, ev.AuthzLatencyUS, "AuthzLatencyUS must be non-zero")
}

// TestIcebergGuarded_EmitsDenyAuditRow_PolicyDeny checks that a policy-denied
// bearer request produces a deny audit.s3 row.
func TestIcebergGuarded_EmitsDenyAuditRow_PolicyDeny(t *testing.T) {
	emitter := audit.NewEmitterWithRingCapacity("test-node", 64)
	authz := denyAllAuthorizer(t)

	base, ks := setupJWTAuthnServer(t,
		WithPolicyAuthorizer(authz),
		WithAuditEmitter(emitter),
	)

	tok := mintBearer(t, ks, "mywh")
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

	time.Sleep(20 * time.Millisecond)

	events := drainAuditEvents(emitter)
	ev, found := findIcebergAuditEvent(events, "iceberg:CommitTable")
	require.True(t, found, "expected deny audit event for iceberg:CommitTable; got events: %+v", events)
	assert.Equal(t, "deny", ev.AuthStatus, "AuthStatus should be deny for policy denial")
	assert.Equal(t, "sa-test", ev.SAID)
	assert.Equal(t, "mywh", ev.Bucket)
	assert.NotEmpty(t, ev.ErrReason, "ErrReason should be populated on deny")
	assert.NotZero(t, ev.AuthzLatencyUS, "AuthzLatencyUS must be non-zero on deny path")
}

// TestIcebergGuarded_EmitsDenyAuditRow_BadToken checks that an invalid bearer
// token produces a deny audit.s3 row with reason="invalid_token".
func TestIcebergGuarded_EmitsDenyAuditRow_BadToken(t *testing.T) {
	emitter := audit.NewEmitterWithRingCapacity("test-node", 64)

	base, _ := setupJWTAuthnServer(t, WithAuditEmitter(emitter))

	req, err := http.NewRequest(http.MethodGet, base+"/iceberg/v1/config?warehouse=wh", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Bearer fake.fake.fake")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	time.Sleep(20 * time.Millisecond)

	events := drainAuditEvents(emitter)
	ev, found := findIcebergAuditEvent(events, "iceberg:GetCatalogConfig")
	require.True(t, found, "expected deny audit event for bad token; got events: %+v", events)
	assert.Equal(t, "deny", ev.AuthStatus)
	assert.Equal(t, "invalid_token", ev.ErrReason)
}

// TestIcebergGuarded_EmitsAnonAllowAuditRow checks that anon-enabled requests
// produce an anon_allow audit.s3 row.
func TestIcebergGuarded_EmitsAnonAllowAuditRow(t *testing.T) {
	emitter := audit.NewEmitterWithRingCapacity("test-node", 64)
	anonCfg := anonConfigReader{"iam.anon-enabled": true}

	dir := t.TempDir()
	backend, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	ks := iamjwt.NewKeySet()
	_, err = ks.GenerateCurrent()
	require.NoError(t, err)

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	startTestServer(t, addr, backend,
		WithJWTKeySet(ks),
		WithBearerConfig(anonCfg),
		WithAuditEmitter(emitter),
	)

	anonBase := "http://" + addr

	req, err := http.NewRequest(http.MethodGet, anonBase+"/iceberg/v1/config?warehouse=default", nil)
	require.NoError(t, err)
	// Send a valid bearer token — anon short-circuit fires before JWT verification.
	tok := mintBearer(t, ks, "default")
	req.Header.Set("Authorization", "Bearer "+tok)

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.NotEqual(t, http.StatusUnauthorized, resp.StatusCode)

	time.Sleep(20 * time.Millisecond)

	events := drainAuditEvents(emitter)
	ev, found := findIcebergAuditEvent(events, "iceberg:GetCatalogConfig")
	require.True(t, found, "expected anon_allow audit event; got events: %+v", events)
	assert.Equal(t, "anon_allow", ev.AuthStatus, "AuthStatus should be anon_allow when iam.anon-enabled=true")

	// SAID should be anonymous (no JWT claims on anon path).
	assert.Empty(t, ev.SAID, "SAID should be empty for anon requests (before normalization assigns '(anonymous)')")
}

// TestIcebergGuarded_SigV4Passthrough_NoAuditFromBearerPath checks that
// requests without a Bearer header (SigV4 path) do not emit a duplicate
// audit row from the Iceberg bearer audit path. Iceberg routes have no
// {bucket} param so the S3 audit envelope middleware is also inactive —
// this is a known gap outside the scope of F29, which targets bearer-gated
// traffic only.
func TestIcebergGuarded_SigV4Passthrough_NoAuditFromBearerPath(t *testing.T) {
	emitter := audit.NewEmitterWithRingCapacity("test-node", 64)

	_ = s3auth.Credentials{} // satisfy import

	base := setupTestServerWithOptions(t, WithAuditEmitter(emitter))

	// No Authorization header — SigV4 path (or anon).
	resp, err := http.Get(base + "/iceberg/v1/config?warehouse=default")
	require.NoError(t, err)
	defer resp.Body.Close()

	time.Sleep(20 * time.Millisecond)

	events := drainAuditEvents(emitter)
	for _, ev := range events {
		assert.NotEqual(t, "iceberg:GetCatalogConfig", ev.Operation,
			"bearer-path audit must NOT fire for SigV4/no-auth Iceberg requests")
	}
}
