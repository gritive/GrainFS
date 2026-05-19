package oauth

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

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
