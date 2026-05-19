package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	auditpkg "github.com/gritive/GrainFS/internal/audit"
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

// TestAuthz_Layer0_ScopeMismatch_403 verifies that a scoped key targeting a
// bucket outside its scope is denied at Layer 0 with reason "key_scope_mismatch".
func TestAuthz_Layer0_ScopeMismatch_403(t *testing.T) {
	cap := &captureAuditEmitter{}
	audit := iam.NewAuditLogger(cap)

	h := newIAMTestHelper(t)
	h.applySACreate(t, "sa-alice")
	// Key is scoped only to "logs".
	h.applyKeyCreateScoped(t, "AK-scoped", "sa-alice", "secret123", []string{"logs"})

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

func TestAuthz_InternalAuditBucket_RejectsRegularSignedReadWithoutIAM(t *testing.T) {
	base, backend := setupTestServerWithBackend(t,
		WithAuth([]s3auth.Credentials{{AccessKey: "AK", SecretKey: "secret"}}),
		WithAuditInternalCredentials("AK-audit-internal", "auditSecret"),
	)
	require.NoError(t, backend.CreateBucket(context.Background(), auditpkg.BucketName))
	_, err := backend.PutObject(context.Background(), auditpkg.BucketName, "metadata/s3/readable.avro", bytes.NewReader([]byte("ok")), "application/octet-stream")
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodGet, base+"/"+auditpkg.BucketName+"/metadata/s3/readable.avro", nil)
	require.NoError(t, err)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "AK", "secret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestAuthz_InternalAuditBucket_ReadArtifactAllowed(t *testing.T) {
	iamAudit := iam.NewAuditLogger(&captureAuditEmitter{})

	base, backend := setupTestServerWithBackend(t,
		WithIAMAudit(iamAudit),
		WithAuth([]s3auth.Credentials{{AccessKey: "AK-admin", SecretKey: "adminSecret"}}),
		WithAuditInternalCredentials("AK-audit-internal", "auditSecret"),
	)
	require.NoError(t, backend.CreateBucket(context.Background(), auditpkg.BucketName))
	_, err := backend.PutObject(context.Background(), auditpkg.BucketName, "metadata/s3/readable.avro", bytes.NewReader([]byte("ok")), "application/octet-stream")
	require.NoError(t, err)

	req, _ := http.NewRequest(http.MethodGet, base+"/"+auditpkg.BucketName+"/metadata/s3/readable.avro", nil)
	req.Host = req.URL.Host
	s3auth.SignRequest(req, "AK-audit-internal", "auditSecret", "us-east-1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)

	assert.Equal(t, http.StatusOK, resp.StatusCode, string(body))
}

func TestAuthz_InternalAuditBucket_RejectsUnsignedLocalRead(t *testing.T) {
	iamAudit := iam.NewAuditLogger(&captureAuditEmitter{})

	base, backend := setupTestServerWithBackend(t,
		WithIAMAudit(iamAudit),
		WithAuth([]s3auth.Credentials{{AccessKey: "AK-admin", SecretKey: "adminSecret"}}),
		WithAuditInternalCredentials("AK-audit-internal", "auditSecret"),
	)
	require.NoError(t, backend.CreateBucket(context.Background(), auditpkg.BucketName))
	_, err := backend.PutObject(context.Background(), auditpkg.BucketName, "metadata/s3/readable.avro", bytes.NewReader([]byte("ok")), "application/octet-stream")
	require.NoError(t, err)

	req, _ := http.NewRequest(http.MethodGet, base+"/"+auditpkg.BucketName+"/metadata/s3/readable.avro", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusForbidden, resp.StatusCode)
}

func TestAuditInternalObjectReadAllowed_LocalhostOnly(t *testing.T) {
	assert.True(t, auditInternalObjectReadAllowed(auditpkg.BucketName, "metadata/s3/file.avro", http.MethodGet, "127.0.0.1:9000", "AK-audit-internal", "AK-audit-internal"))
	assert.True(t, auditInternalObjectReadAllowed(auditpkg.BucketName, "data/2026-05-15/file.parquet", http.MethodHead, "[::1]:9000", "AK-audit-internal", "AK-audit-internal"))

	assert.False(t, auditInternalObjectReadAllowed(auditpkg.BucketName, "metadata/s3/file.avro", http.MethodGet, "127.0.0.1:9000", "", "AK-audit-internal"))
	assert.False(t, auditInternalObjectReadAllowed(auditpkg.BucketName, "metadata/s3/file.avro", http.MethodGet, "127.0.0.1:9000", "AK-admin", "AK-audit-internal"))
	assert.False(t, auditInternalObjectReadAllowed(auditpkg.BucketName, "metadata/s3/file.avro", http.MethodGet, "203.0.113.10:50000", "AK-audit-internal", "AK-audit-internal"))
	assert.False(t, auditInternalObjectReadAllowed(auditpkg.BucketName, "", http.MethodGet, "127.0.0.1:9000", "AK-audit-internal", "AK-audit-internal"))
	assert.False(t, auditInternalObjectReadAllowed("user-bucket", "metadata/s3/file.avro", http.MethodGet, "127.0.0.1:9000", "AK-audit-internal", "AK-audit-internal"))
	assert.False(t, auditInternalObjectReadAllowed(auditpkg.BucketName, "metadata/s3/file.avro", http.MethodPut, "127.0.0.1:9000", "AK-audit-internal", "AK-audit-internal"))
}

func TestAuditObjectReadRequest_AllowsSignedRemoteAuthorizationPath(t *testing.T) {
	assert.True(t, auditObjectReadRequest(auditpkg.BucketName, "metadata/s3/file.avro", http.MethodGet))
	assert.True(t, auditObjectReadRequest(auditpkg.BucketName, "data/2026-05-15/file.parquet", http.MethodHead))

	assert.False(t, auditObjectReadRequest(auditpkg.BucketName, "", http.MethodGet))
	assert.False(t, auditObjectReadRequest(auditpkg.BucketName, "metadata/s3/file.avro", http.MethodPut))
	assert.False(t, auditObjectReadRequest("user-bucket", "metadata/s3/file.avro", http.MethodGet))
}
