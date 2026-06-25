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
	"github.com/gritive/GrainFS/internal/storage"
)

func TestS3ActionEnum(t *testing.T) {
	tests := []struct {
		method        string
		path          string
		hasKey        bool
		hasPolicy     bool
		hasVersioning bool
		hasVersions   bool
		hasRetention  bool
		hasObjectLock bool
		hasLifecycle  bool
		want          s3auth.S3Action
	}{
		{"GET", "/bucket/key", true, false, false, false, false, false, false, s3auth.GetObject},
		{"GET", "/bucket", false, false, false, false, false, false, false, s3auth.ListBucket},
		{"HEAD", "/bucket/key", true, false, false, false, false, false, false, s3auth.HeadObject},
		{"HEAD", "/bucket", false, false, false, false, false, false, false, s3auth.ListBucket},
		{"PUT", "/bucket/key", true, false, false, false, false, false, false, s3auth.PutObject},
		{"PUT", "/bucket", false, false, false, false, false, false, false, s3auth.CreateBucket},
		{"DELETE", "/bucket/key", true, false, false, false, false, false, false, s3auth.DeleteObject},
		{"DELETE", "/bucket", false, false, false, false, false, false, false, s3auth.DeleteBucket},
		{"POST", "/bucket/key", true, false, false, false, false, false, false, s3auth.PutObject}, // multipart
		{"UNKNOWN", "/bucket", false, false, false, false, false, false, false, s3auth.UnknownAction},
		// Phase 5d #4: ?policy CRUD maps to dedicated S3Actions.
		{"GET", "/bucket", false, true, false, false, false, false, false, s3auth.GetBucketPolicy},
		{"PUT", "/bucket", false, true, false, false, false, false, false, s3auth.PutBucketPolicy},
		{"DELETE", "/bucket", false, true, false, false, false, false, false, s3auth.DeleteBucketPolicy},
		{"GET", "/bucket", false, false, true, false, false, false, false, s3auth.GetBucketVersioning},
		{"PUT", "/bucket", false, false, true, false, false, false, false, s3auth.PutBucketVersioning},
		{"GET", "/bucket", false, false, false, true, false, false, false, s3auth.ListBucketVersions},
		{"GET", "/bucket/key", true, false, false, false, true, false, false, s3auth.GetObjectRetention},
		{"PUT", "/bucket/key", true, false, false, false, true, false, false, s3auth.PutObjectRetention},
		{"GET", "/bucket", false, false, false, false, false, true, false, s3auth.GetBucketObjectLockConfiguration},
		// R2: ?lifecycle subresource maps to dedicated S3Actions (bucket-scoped only).
		{"GET", "/bucket", false, false, false, false, false, false, true, s3auth.GetBucketLifecycleConfiguration},
		{"PUT", "/bucket", false, false, false, false, false, false, true, s3auth.PutBucketLifecycleConfiguration},
		{"DELETE", "/bucket", false, false, false, false, false, false, true, s3auth.DeleteBucketLifecycleConfiguration},
	}

	for _, tt := range tests {
		t.Run(tt.method+"_hasKey="+boolStr(tt.hasKey)+"_hasPolicy="+boolStr(tt.hasPolicy)+"_hasLifecycle="+boolStr(tt.hasLifecycle), func(t *testing.T) {
			got := s3ActionEnum(tt.method, tt.path, tt.hasKey, tt.hasPolicy, tt.hasVersioning, tt.hasVersions, tt.hasRetention, tt.hasObjectLock, tt.hasLifecycle)
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
	enc     storage.DataEncryptor
}

func newIAMTestHelper(t *testing.T) *iamTestHelper {
	t.Helper()
	clusterID := bytes.Repeat([]byte{0xcd}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0xab}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	de := storage.NewDEKKeeperAdapter(keeper, clusterID)
	store := iam.NewStore()
	ap := iam.NewApplier(store, de)
	return &iamTestHelper{store: store, applier: ap, enc: de}
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
	wrapped, gen, err := iam.WrapSecret(h.enc, saID, ak, secret)
	require.NoError(t, err)
	_ = gen // DEK adapter seals at active gen 0; KeyCreatePayload defaults to 0

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
