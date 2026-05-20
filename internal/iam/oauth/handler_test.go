package oauth

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// --- fakes ---

type fakeSAResolver struct {
	records map[string]saRecord // accessKey → record
	revoked map[string]bool     // saID → revoked
}

type saRecord struct {
	saID      string
	secretKey string
}

func newFakeSAResolver() *fakeSAResolver {
	return &fakeSAResolver{
		records: make(map[string]saRecord),
		revoked: make(map[string]bool),
	}
}

func (f *fakeSAResolver) add(accessKey, saID, secretKey string) {
	f.records[accessKey] = saRecord{saID: saID, secretKey: secretKey}
}

func (f *fakeSAResolver) revoke(saID string) { f.revoked[saID] = true }

func (f *fakeSAResolver) LookupByAccessKey(_ context.Context, accessKey string) (string, []byte, error) {
	rec, ok := f.records[accessKey]
	if !ok {
		return "", nil, errUnknownOrRevoked
	}
	if f.revoked[rec.saID] {
		return "", nil, errUnknownOrRevoked
	}
	return rec.saID, []byte(rec.secretKey), nil
}

type fakeAuthorizer struct {
	deny map[string]bool // saID → deny
}

func newFakeAuthorizer() *fakeAuthorizer { return &fakeAuthorizer{deny: make(map[string]bool)} }

func (a *fakeAuthorizer) denyAll(saID string)    { a.deny[saID] = true }
func (a *fakeAuthorizer) allowAll(_ string) bool { return true }

func (a *fakeAuthorizer) Authorize(_ context.Context, saID, _ string, _ policy.RequestContext) policy.EvalResult {
	if a.deny[saID] {
		return policy.EvalResult{Decision: policy.DecisionDeny, Reason: "test deny"}
	}
	return policy.EvalResult{Decision: policy.DecisionAllow}
}

// --- test helpers ---

type testState struct {
	sa    *fakeSAResolver
	authz *fakeAuthorizer
	keys  *iamjwt.KeySet
	h     *Handler
}

func newTestHandler(t *testing.T) *testState {
	t.Helper()
	sa := newFakeSAResolver()
	sa.add("AKIA-test", "sa-test", "secret")

	authz := newFakeAuthorizer()

	ks := iamjwt.NewKeySet()
	if _, err := ks.GenerateCurrent(); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return &testState{
		sa:    sa,
		authz: authz,
		keys:  ks,
		h:     NewHandler(sa, ks, authz),
	}
}

func (ts *testState) revokeSA(saID string) { ts.sa.revoke(saID) }
func (ts *testState) attachPolicy(saID, policy string) {
	// For TestOAuth_TokenMintDeniedWhenSALacksGetCatalogConfig the policy name is
	// irrelevant; what matters is that fakeAuthorizer denies the SA.
	ts.authz.denyAll(saID)
	_ = policy
}

func doPost(h *Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest("POST", "/iceberg/v1/oauth/tokens", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

// --- spec tests ---

func TestOAuth_FormBodyMint(t *testing.T) {
	ts := newTestHandler(t)
	w := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE:analytics")

	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"access_token"`) {
		t.Fatal("response missing access_token")
	}
	if !strings.Contains(w.Body.String(), `"bearer"`) {
		t.Fatal(`token_type must be "bearer" (DuckDB #18483)`)
	}
}

func TestOAuth_FormBodyMintInitializesMissingSigningKey(t *testing.T) {
	ts := newTestHandler(t)
	ks := iamjwt.NewKeySet()
	h := NewHandler(ts.sa, ks, ts.authz)

	w := doPost(h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE:analytics")

	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), `"access_token"`) {
		t.Fatal("response missing access_token")
	}
}

func TestOAuth_HTTPBasicMint(t *testing.T) {
	ts := newTestHandler(t)
	req := httptest.NewRequest("POST", "/iceberg/v1/oauth/tokens",
		strings.NewReader("grant_type=client_credentials&scope=PRINCIPAL_ROLE:analytics"))
	req.SetBasicAuth("AKIA-test", "secret")
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	ts.h.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("code = %d, body = %s", w.Code, w.Body.String())
	}
}

func TestOAuth_WrongSecret_401(t *testing.T) {
	ts := newTestHandler(t)
	w := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=WRONG&scope=PRINCIPAL_ROLE:analytics")

	if w.Code != 401 {
		t.Fatalf("code = %d, want 401", w.Code)
	}
}

func TestOAuth_SARevoked_401(t *testing.T) {
	ts := newTestHandler(t)
	ts.revokeSA("sa-test")
	w := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE:analytics")

	if w.Code != 401 {
		t.Fatalf("code = %d, want 401 (F#5)", w.Code)
	}
}

func TestOAuth_GrantTypeRequired(t *testing.T) {
	ts := newTestHandler(t)
	w := doPost(ts.h,
		"client_id=AKIA-test&client_secret=secret")

	if w.Code != 400 {
		t.Fatalf("missing grant_type = %d, want 400", w.Code)
	}
}

func TestOAuth_TokenMintDeniedWhenSALacksGetCatalogConfig(t *testing.T) {
	ts := newTestHandler(t)
	ts.attachPolicy("sa-test", "writeonly")
	w := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE:analytics")

	if w.Code != 403 {
		t.Fatalf("code = %d, want 403 (SA lacks iceberg:GetCatalogConfig)", w.Code)
	}
}

// TestOAuth_UnknownAndWrongSecret_SameErrorBody verifies that both unknown
// access_key and wrong secret return identical 401 + JSON body — so neither
// path can be used to enumerate valid access_keys via response inspection (F8).
func TestOAuth_UnknownAndWrongSecret_SameErrorBody(t *testing.T) {
	ts := newTestHandler(t)

	unknownW := doPost(ts.h,
		"grant_type=client_credentials&client_id=UNKNOWN-KEY&client_secret=anything&scope=PRINCIPAL_ROLE:analytics")
	wrongW := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=WRONG&scope=PRINCIPAL_ROLE:analytics")

	require.Equal(t, 401, unknownW.Code, "unknown key must return 401")
	require.Equal(t, 401, wrongW.Code, "wrong secret must return 401")

	var unknownBody, wrongBody map[string]string
	require.NoError(t, json.Unmarshal(unknownW.Body.Bytes(), &unknownBody))
	require.NoError(t, json.Unmarshal(wrongW.Body.Bytes(), &wrongBody))

	assert.Equal(t, unknownBody["error"], wrongBody["error"],
		"error field must be identical for unknown-key and wrong-secret paths")
	assert.Equal(t, unknownBody["error_description"], wrongBody["error_description"],
		"error_description must be identical for unknown-key and wrong-secret paths")
}

// TestOAuth_MultiPrincipalRole_400 verifies that a scope with more than one
// PRINCIPAL_ROLE: token is rejected with 400 invalid_scope (F11).
func TestOAuth_MultiPrincipalRole_400(t *testing.T) {
	ts := newTestHandler(t)
	// URL-encode the space-separated scope so form parsing produces two tokens.
	w := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE%3Awh-a+PRINCIPAL_ROLE%3Awh-b")

	require.Equal(t, 400, w.Code, "dual PRINCIPAL_ROLE tokens must return 400")

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "invalid_scope", body["error"])
	assert.Contains(t, body["error_description"], "exactly one PRINCIPAL_ROLE")
}

// TestOAuth_SubIsaSAID verifies that the minted JWT encodes sa_id as Sub,
// not the access_key.
func TestOAuth_SubIsSAID(t *testing.T) {
	ts := newTestHandler(t)
	w := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE:analytics")

	if w.Code != 200 {
		t.Fatalf("code = %d", w.Code)
	}

	// Extract access_token from JSON body.
	body := w.Body.String()
	prefix := `"access_token":"`
	i := strings.Index(body, prefix)
	if i < 0 {
		t.Fatal("access_token not found in response")
	}
	rest := body[i+len(prefix):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		t.Fatal("malformed access_token in response")
	}
	tok := rest[:j]

	claims, err := ts.keys.Verify(tok)
	if err != nil {
		t.Fatalf("verify token: %v", err)
	}
	if claims.Sub != "sa-test" {
		t.Errorf("Sub = %q, want %q (sa_id)", claims.Sub, "sa-test")
	}
	if claims.Warehouse != "analytics" {
		t.Errorf("Warehouse = %q, want %q", claims.Warehouse, "analytics")
	}
}

// TestOAuth_EmptyPrincipalRole_400 verifies that scope=PRINCIPAL_ROLE: (empty
// value after colon) is rejected with 400 invalid_scope (F23).
func TestOAuth_EmptyPrincipalRole_400(t *testing.T) {
	ts := newTestHandler(t)
	w := doPost(ts.h,
		"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope=PRINCIPAL_ROLE%3A")

	require.Equal(t, 400, w.Code, "empty PRINCIPAL_ROLE must return 400")

	var body map[string]string
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	assert.Equal(t, "invalid_scope", body["error"])
	assert.Contains(t, body["error_description"], "non-empty warehouse")
}

// TestOAuth_URIShapedWarehouse_400 verifies that URI/path-shaped warehouse
// names in PRINCIPAL_ROLE scope are rejected with 400 invalid_scope (F24).
// Table-driven: s3://, http://, path-with-slash, and .. traversal.
func TestOAuth_URIShapedWarehouse_400(t *testing.T) {
	ts := newTestHandler(t)

	cases := []struct {
		name  string
		scope string
		desc  string
	}{
		{
			name:  "s3_URI",
			scope: "PRINCIPAL_ROLE:s3://attacker.com/bucket",
			desc:  "s3:// URI must be rejected",
		},
		{
			name:  "http_URI",
			scope: "PRINCIPAL_ROLE:http://evil.example.com/x",
			desc:  "http:// URI must be rejected",
		},
		{
			name:  "https_URI",
			scope: "PRINCIPAL_ROLE:https://evil.example.com/x",
			desc:  "https:// URI must be rejected",
		},
		{
			name:  "path_with_slash",
			scope: "PRINCIPAL_ROLE:warehouse/subdir",
			desc:  "slash-separated path must be rejected",
		},
		{
			name:  "dotdot_traversal",
			scope: "PRINCIPAL_ROLE:..evil",
			desc:  ".. traversal must be rejected",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// URL-encode the colon so form parsing treats it as a single param value.
			encodedScope := strings.ReplaceAll(tc.scope, ":", "%3A")
			encodedScope = strings.ReplaceAll(encodedScope, "/", "%2F")
			w := doPost(ts.h,
				"grant_type=client_credentials&client_id=AKIA-test&client_secret=secret&scope="+encodedScope)

			require.Equal(t, 400, w.Code, "%s: want 400, got %d (body=%s)", tc.desc, w.Code, w.Body.String())

			var body map[string]string
			require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
			assert.Equal(t, "invalid_scope", body["error"], tc.desc)
		})
	}
}
