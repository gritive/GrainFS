package server

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/iam/iampb"
	"github.com/gritive/GrainFS/internal/s3auth"
)

func TestS3ActionEnum(t *testing.T) {
	tests := []struct {
		method    string
		path      string
		hasKey    bool
		hasPolicy bool
		want      s3auth.S3Action
	}{
		{"GET", "/bucket/key", true, false, s3auth.GetObject},
		{"GET", "/bucket", false, false, s3auth.ListBucket},
		{"HEAD", "/bucket/key", true, false, s3auth.HeadObject},
		{"HEAD", "/bucket", false, false, s3auth.ListBucket},
		{"PUT", "/bucket/key", true, false, s3auth.PutObject},
		{"PUT", "/bucket", false, false, s3auth.CreateBucket},
		{"DELETE", "/bucket/key", true, false, s3auth.DeleteObject},
		{"DELETE", "/bucket", false, false, s3auth.DeleteBucket},
		{"POST", "/bucket/key", true, false, s3auth.PutObject}, // multipart
		{"UNKNOWN", "/bucket", false, false, s3auth.UnknownAction},
		// Phase 5d #4: ?policy CRUD maps to dedicated S3Actions.
		{"GET", "/bucket", false, true, s3auth.GetBucketPolicy},
		{"PUT", "/bucket", false, true, s3auth.PutBucketPolicy},
		{"DELETE", "/bucket", false, true, s3auth.DeleteBucketPolicy},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_hasKey="+boolStr(tt.hasKey)+"_hasPolicy="+boolStr(tt.hasPolicy), func(t *testing.T) {
			got := s3ActionEnum(tt.method, tt.path, tt.hasKey, tt.hasPolicy)
			assert.Equal(t, tt.want, got)
		})
	}
}

func boolStr(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// --- Layer 0 scope filter tests ---

// captureAuditEmitter records the last deny reason emitted.
type captureAuditEmitter struct {
	mu      sync.Mutex
	reasons []string
}

func (c *captureAuditEmitter) Emit(_ context.Context, ev iam.AuditEvent) error {
	if ev.Status == iam.AuditStatusDeny {
		c.mu.Lock()
		c.reasons = append(c.reasons, ev.Reason)
		c.mu.Unlock()
	}
	return nil
}

func (c *captureAuditEmitter) lastReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.reasons) == 0 {
		return ""
	}
	return c.reasons[len(c.reasons)-1]
}

// iamTestHelper holds the pieces needed to set up an IAM-enabled test server.
type iamTestHelper struct {
	store   *iam.Store
	applier *iam.Applier
	enc     *encrypt.Encryptor
}

func newIAMTestHelper(t *testing.T) *iamTestHelper {
	t.Helper()
	key := bytes.Repeat([]byte{0xab}, 32)
	enc, err := encrypt.NewEncryptor(key)
	require.NoError(t, err)
	store := iam.NewStore()
	ap := iam.NewApplier(store, enc)
	return &iamTestHelper{store: store, applier: ap, enc: enc}
}

func (h *iamTestHelper) applySACreate(t *testing.T, saID string) {
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
	require.NoError(t, h.applier.ApplySACreate(b.FinishedBytes()))
}

func (h *iamTestHelper) applyGrantPut(t *testing.T, saID, bucket string, role iam.Role) {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	bkOff := b.CreateString(bucket)
	cbOff := b.CreateString("")
	iampb.GrantPutPayloadStart(b)
	iampb.GrantPutPayloadAddSaId(b, saOff)
	iampb.GrantPutPayloadAddBucket(b, bkOff)
	iampb.GrantPutPayloadAddRole(b, iampb.Role(role))
	iampb.GrantPutPayloadAddCreatedAtUnixNs(b, time.Now().UnixNano())
	iampb.GrantPutPayloadAddCreatedBy(b, cbOff)
	end := iampb.GrantPutPayloadEnd(b)
	b.Finish(end)
	require.NoError(t, h.applier.ApplyGrantPut(b.FinishedBytes()))
}

func (h *iamTestHelper) applyGrantWildcardPut(t *testing.T, saID string, role iam.Role) {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	saOff := b.CreateString(saID)
	cbOff := b.CreateString("")
	iampb.GrantWildcardPutPayloadStart(b)
	iampb.GrantWildcardPutPayloadAddSaId(b, saOff)
	iampb.GrantWildcardPutPayloadAddRole(b, iampb.Role(role))
	iampb.GrantWildcardPutPayloadAddCreatedAtUnixNs(b, time.Now().UnixNano())
	iampb.GrantWildcardPutPayloadAddCreatedBy(b, cbOff)
	end := iampb.GrantWildcardPutPayloadEnd(b)
	b.Finish(end)
	require.NoError(t, h.applier.ApplyGrantWildcardPut(b.FinishedBytes()))
}

func (h *iamTestHelper) applyKeyCreateScoped(t *testing.T, ak, saID, secret string, scope []string) {
	t.Helper()
	wrapped, err := iam.WrapSecret(h.enc, saID, secret)
	require.NoError(t, err)

	b := flatbuffers.NewBuilder(256)
	akOff := b.CreateString(ak)
	saOff := b.CreateString(saID)
	encOff := b.CreateByteVector(wrapped)
	scopeOffsets := make([]flatbuffers.UOffsetT, len(scope))
	for i, s := range scope {
		scopeOffsets[i] = b.CreateString(s)
	}
	iampb.KeyCreatePayloadStartBucketScopeVector(b, len(scope))
	for i := len(scopeOffsets) - 1; i >= 0; i-- {
		b.PrependUOffsetT(scopeOffsets[i])
	}
	scopeVec := b.EndVector(len(scope))
	iampb.KeyCreatePayloadStart(b)
	iampb.KeyCreatePayloadAddAccessKey(b, akOff)
	iampb.KeyCreatePayloadAddSecretKeyEnc(b, encOff)
	iampb.KeyCreatePayloadAddSaId(b, saOff)
	iampb.KeyCreatePayloadAddCreatedAtUnixNs(b, time.Now().UnixNano())
	iampb.KeyCreatePayloadAddBucketScope(b, scopeVec)
	end := iampb.KeyCreatePayloadEnd(b)
	b.Finish(end)
	require.NoError(t, h.applier.ApplyKeyCreateScoped(b.FinishedBytes()))
}

func (h *iamTestHelper) applyKeyCreate(t *testing.T, ak, saID, secret string) {
	t.Helper()
	wrapped, err := iam.WrapSecret(h.enc, saID, secret)
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
	require.NoError(t, h.applier.ApplyKeyCreate(b.FinishedBytes()))
}

func (h *iamTestHelper) applyAuthEnable(t *testing.T) {
	t.Helper()
	require.NoError(t, h.applier.ApplyAuthEnable(nil))
}

// TestAuthz_Layer0_ScopeMismatch_403 verifies that a scoped key targeting a
// bucket outside its scope is denied at Layer 0 with reason "key_scope_mismatch".
func TestAuthz_Layer0_ScopeMismatch_403(t *testing.T) {
	cap := &captureAuditEmitter{}
	audit := iam.NewAuditLogger(cap)

	h := newIAMTestHelper(t)
	h.applySACreate(t, "sa-alice")
	// SA has grant on "logs" (which key scope allows), but NOT "reports".
	h.applyGrantPut(t, "sa-alice", "logs", iam.RoleAdmin)
	// Key is scoped only to "logs".
	h.applyKeyCreateScoped(t, "AK-scoped", "sa-alice", "secret123", []string{"logs"})
	h.applyAuthEnable(t)

	base := setupTestServerWithOptions(t,
		WithIAMStore(h.store),
		WithIAMAudit(audit),
		// SigV4 verifier — must match key used to sign requests.
		WithAuth([]s3auth.Credentials{{AccessKey: "AK-scoped", SecretKey: "secret123"}}),
	)

	// PUT /reports — bucket NOT in key scope ["logs"] → Layer 0 deny.
	req, _ := http.NewRequest(http.MethodPut, base+"/reports", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "AK-scoped", "secret123", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
	assert.Equal(t, "key_scope_mismatch", cap.lastReason())
}

// TestAuthz_Layer0_ScopeMatch_PassToLayer1 verifies that a scoped key targeting
// a bucket within its scope passes Layer 0 and proceeds to Layer 1.
func TestAuthz_Layer0_ScopeMatch_PassToLayer1(t *testing.T) {
	cap := &captureAuditEmitter{}
	audit := iam.NewAuditLogger(cap)

	h := newIAMTestHelper(t)
	h.applySACreate(t, "sa-alice")
	// Grant RoleAdmin on "logs" so Layer 1 allows the request.
	h.applyGrantPut(t, "sa-alice", "logs", iam.RoleAdmin)
	// Key scoped to "logs".
	h.applyKeyCreateScoped(t, "AK-scoped", "sa-alice", "secret123", []string{"logs"})
	h.applyAuthEnable(t)

	base := setupTestServerWithOptions(t,
		WithIAMStore(h.store),
		WithIAMAudit(audit),
		WithAuth([]s3auth.Credentials{{AccessKey: "AK-scoped", SecretKey: "secret123"}}),
	)

	// PUT /logs — bucket IS in scope → Layer 0 passes, Layer 1 has Admin grant → 200.
	req, _ := http.NewRequest(http.MethodPut, base+"/logs", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "AK-scoped", "secret123", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	// No deny emitted — scope passed and Layer 1 allowed.
	assert.Empty(t, cap.lastReason())
}

// TestAuthz_Layer0_NilScope_PassToLayer1 verifies that a legacy key (nil
// BucketScope) passes Layer 0 unrestricted; Layer 1 still gates.
func TestAuthz_Layer0_NilScope_PassToLayer1(t *testing.T) {
	cap := &captureAuditEmitter{}
	audit := iam.NewAuditLogger(cap)

	h := newIAMTestHelper(t)
	h.applySACreate(t, "sa-legacy")
	// Wildcard Admin grant → Layer 1 will allow any bucket.
	h.applyGrantWildcardPut(t, "sa-legacy", iam.RoleAdmin)
	// Legacy key with nil scope.
	h.applyKeyCreate(t, "AK-legacy", "sa-legacy", "secret456")
	h.applyAuthEnable(t)

	base := setupTestServerWithOptions(t,
		WithIAMStore(h.store),
		WithIAMAudit(audit),
		WithAuth([]s3auth.Credentials{{AccessKey: "AK-legacy", SecretKey: "secret456"}}),
	)

	// PUT /anybucket — nil scope → Layer 0 passes, wildcard Layer 1 allows → 200.
	req, _ := http.NewRequest(http.MethodPut, base+"/anybucket", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "AK-legacy", "secret456", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Empty(t, cap.lastReason())
}
