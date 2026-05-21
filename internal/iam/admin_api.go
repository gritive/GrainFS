package iam

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// SACreateRequest is the JSON body for POST /admin/iam/sa.
// Note: external admin HTTP can use JSON; only cluster-internal RPC uses
// FlatBuffers (CLAUDE.md "내부 통신 JSON 미사용" applies to raft RPC, not admin).
type SACreateRequest struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// SACreateResponse returns the new SA along with the access_key/secret_key
// pair created for it. SecretKey is plaintext, returned ONCE.
type SACreateResponse struct {
	SAID      string    `json:"sa_id"`
	Name      string    `json:"name"`
	AccessKey string    `json:"access_key"`
	SecretKey string    `json:"secret_key"`
	CreatedAt time.Time `json:"created_at"`
}

type SAListItem struct {
	SAID        string    `json:"sa_id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	NumKeys     int       `json:"num_keys"`
}

type GrantPutRequest struct {
	SAID   string `json:"sa_id"`
	Bucket string `json:"bucket"`
	Role   string `json:"role"`
}

type GrantDeleteRequest struct {
	SAID   string `json:"sa_id"`
	Bucket string `json:"bucket"`
}

// PostureChecker is an optional dependency that the AdminAPI consults before
// performing the FIRST SA create on an empty store. CheckAnonOff must return
// nil when the node-local TLS posture (server cert on disk OR trusted-proxy
// CIDR set) is safe to drive iam.anon-enabled → false, or an error whose
// Error() string carries an operator-facing remediation hint otherwise.
//
// F#26-tls-posture: the first SA create atomically flips iam.anon-enabled to
// false at FSM apply time (see internal/cluster/meta_fsm.go:applyIAMSACreate).
// The flip's reload hook refuses if no TLS cert and no trusted-proxy CIDR are
// configured, but the SA itself is already committed by then — leaving the
// cluster in a "SA exists, anon still on" silent-violation state. The
// PostureChecker rejects the admin UDS RPC before propose so the operator
// hears about it instead of discovering anon stuck open later.
type PostureChecker interface {
	CheckAnonOff(ctx context.Context) error
}

// AdminAPI hosts HTTP handlers for /admin/iam/* endpoints. Stdlib handlers
// are wrapped onto Hertz at the admin UDS in Task 21.
type AdminAPI struct {
	store    *Store
	proposer Proposer
	enc      *encrypt.Encryptor
	posture  PostureChecker // optional; nil = skip first-SA pre-check (legacy/test default)
}

func NewAdminAPI(store *Store, proposer Proposer, enc *encrypt.Encryptor) *AdminAPI {
	return &AdminAPI{store: store, proposer: proposer, enc: enc}
}

// SetPostureChecker installs the optional PostureChecker. Wired by
// internal/serveruntime at boot; left nil in unit tests that don't exercise
// the F#26 pre-check.
func (a *AdminAPI) SetPostureChecker(pc PostureChecker) { a.posture = pc }

// CreateSA creates a new ServiceAccount and an initial access key for it.
// Returns *adminapi.Error on validation or conflict; use errors.As to inspect.
func (a *AdminAPI) CreateSA(ctx context.Context, req SACreateRequest) (SACreateResponse, error) {
	if req.Name == "" {
		return SACreateResponse{}, &adminapi.Error{Code: "invalid", Message: "name required"}
	}
	// F#26-tls-posture: gate the first SA create on local TLS posture. The FSM
	// applies the SA AND atomically flips iam.anon-enabled → false; if posture
	// is bad the flip's reload hook would refuse and silently leave anon on.
	// Reject the RPC up front with the remediation hint instead. Subsequent
	// SA creates (store non-empty) skip the check — they don't trigger the flip.
	if a.posture != nil && a.store.IsEmpty() {
		if err := a.posture.CheckAnonOff(ctx); err != nil {
			return SACreateResponse{}, &adminapi.Error{Code: "precondition", Message: err.Error()}
		}
	}
	now := time.Now().UTC()
	accessKey, secretKey := genCredentialPair()
	sa := ServiceAccount{
		ID: NewUUIDv7(), Name: req.Name, Description: req.Description,
		CreatedAt: now, CreatedBy: PrincipalFromContext(ctx),
	}
	if err := a.proposer.ProposeSACreate(ctx, sa); err != nil {
		return SACreateResponse{}, &adminapi.Error{Code: "internal", Message: "propose SA: " + err.Error()}
	}
	wrapped, err := WrapSecret(a.enc, sa.ID, secretKey)
	if err != nil {
		return SACreateResponse{}, &adminapi.Error{Code: "internal", Message: "wrap secret: " + err.Error()}
	}
	k := AccessKey{
		AccessKey: accessKey, SecretKey: secretKey, SecretKeyEnc: wrapped,
		SAID: sa.ID, Status: KeyStatusActive, CreatedAt: now,
	}
	if err := a.proposer.ProposeKeyCreate(ctx, k); err != nil {
		return SACreateResponse{}, &adminapi.Error{Code: "internal", Message: "propose key: " + err.Error()}
	}
	return SACreateResponse{SAID: sa.ID, Name: sa.Name, AccessKey: accessKey, SecretKey: secretKey, CreatedAt: now}, nil
}

func (a *AdminAPI) HandleSACreate(w http.ResponseWriter, r *http.Request) {
	var req SACreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	resp, err := a.CreateSA(r.Context(), req)
	if err != nil {
		writeAdminError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (a *AdminAPI) ListSA(_ context.Context) ([]SAListItem, error) {
	st := a.store.snapshot()
	out := make([]SAListItem, 0, len(st.sas))
	for id, sa := range st.sas {
		nKeys := 0
		for _, k := range st.keysByAK {
			if k.SAID == id {
				nKeys++
			}
		}
		out = append(out, SAListItem{SAID: sa.ID, Name: sa.Name, Description: sa.Description, CreatedAt: sa.CreatedAt, NumKeys: nKeys})
	}
	return out, nil
}

func (a *AdminAPI) HandleSAList(w http.ResponseWriter, r *http.Request) {
	out, _ := a.ListSA(r.Context())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (a *AdminAPI) PutGrant(ctx context.Context, req GrantPutRequest) error {
	if req.SAID == "" {
		return &adminapi.Error{Code: "invalid", Message: "sa_id required"}
	}
	if req.Bucket == "" {
		return &adminapi.Error{Code: "invalid", Message: "bucket required"}
	}
	policy, err := grantRolePolicy(req.Role)
	if err != nil {
		return err
	}
	if _, ok := a.store.LookupSA(req.SAID); !ok {
		return &adminapi.Error{Code: "not_found", Message: "SA not found"}
	}
	if err := a.proposer.ProposePolicyAttachToSAPut(ctx, req.SAID, policy); err != nil {
		return &adminapi.Error{Code: "internal", Message: "attach policy: " + err.Error()}
	}
	return nil
}

func (a *AdminAPI) DeleteGrant(ctx context.Context, req GrantDeleteRequest) error {
	if req.SAID == "" {
		return &adminapi.Error{Code: "invalid", Message: "sa_id required"}
	}
	if req.Bucket == "" {
		return &adminapi.Error{Code: "invalid", Message: "bucket required"}
	}
	for _, policy := range []string{"readonly", "writeonly", "bucket-admin"} {
		if err := a.proposer.ProposePolicyAttachToSADelete(ctx, req.SAID, policy); err != nil {
			return &adminapi.Error{Code: "internal", Message: "detach policy: " + err.Error()}
		}
	}
	return nil
}

func grantRolePolicy(role string) (string, error) {
	switch strings.ToLower(role) {
	case "read":
		return "readonly", nil
	case "write":
		return "writeonly", nil
	case "admin":
		return "bucket-admin", nil
	default:
		return "", &adminapi.Error{Code: "invalid", Message: "role must be Read, Write, or Admin"}
	}
}

// SAGetResponse is the wire shape for GET /admin/iam/sa/{id}. Mirrors
// SAListItem field naming so list and detail outputs stay parseable by
// the same client.
type SAGetResponse struct {
	SAID        string    `json:"sa_id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	CreatedBy   string    `json:"created_by,omitempty"`
}

func (a *AdminAPI) GetSA(_ context.Context, saID string) (SAGetResponse, error) {
	sa, ok := a.store.LookupSA(saID)
	if !ok {
		return SAGetResponse{}, &adminapi.Error{Code: "not_found", Message: "SA not found"}
	}
	return SAGetResponse{SAID: sa.ID, Name: sa.Name, Description: sa.Description, CreatedAt: sa.CreatedAt, CreatedBy: sa.CreatedBy}, nil
}

func (a *AdminAPI) HandleSAGet(w http.ResponseWriter, r *http.Request, saID string) {
	resp, err := a.GetSA(r.Context(), saID)
	if err != nil {
		writeAdminError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (a *AdminAPI) DeleteSA(ctx context.Context, saID string) error {
	if _, ok := a.store.LookupSA(saID); !ok {
		return &adminapi.Error{Code: "not_found", Message: "SA not found"}
	}
	if err := a.proposer.ProposeSADelete(ctx, saID); err != nil {
		return &adminapi.Error{Code: "internal", Message: "propose: " + err.Error()}
	}
	return nil
}

func (a *AdminAPI) HandleSADelete(w http.ResponseWriter, r *http.Request, saID string) {
	if err := a.DeleteSA(r.Context(), saID); err != nil {
		writeAdminError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// KeyCreateRequest is the JSON body for POST /admin/iam/sa/{id}/key.
// Empty body is allowed; ExpiresAt nil = never.
// Buckets, when non-empty, restricts the key to those buckets only.
type KeyCreateRequest struct {
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Buckets   []string   `json:"buckets,omitempty"`
}

// KeyCreateResponse returns the rotated key with one-time plaintext secret.
// Buckets echoes the normalized scope if the key is bucket-scoped.
type KeyCreateResponse struct {
	AccessKey string     `json:"access_key"`
	SecretKey string     `json:"secret_key"`
	SAID      string     `json:"sa_id"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	Buckets   []string   `json:"buckets,omitempty"`
}

func (a *AdminAPI) CreateKey(ctx context.Context, saID string, req KeyCreateRequest) (KeyCreateResponse, error) {
	if _, ok := a.store.LookupSA(saID); !ok {
		return KeyCreateResponse{}, &adminapi.Error{Code: "not_found", Message: "SA not found"}
	}
	scope, err := NormalizeScope(req.Buckets)
	if err != nil {
		return KeyCreateResponse{}, &adminapi.Error{Code: "invalid", Message: err.Error()}
	}
	accessKey, secretKey := genCredentialPair()
	wrapped, err := WrapSecret(a.enc, saID, secretKey)
	if err != nil {
		return KeyCreateResponse{}, &adminapi.Error{Code: "internal", Message: "wrap: " + err.Error()}
	}
	k := AccessKey{
		AccessKey: accessKey, SecretKey: secretKey, SecretKeyEnc: wrapped,
		SAID: saID, Status: KeyStatusActive, CreatedAt: time.Now().UTC(),
		ExpiresAt: req.ExpiresAt, BucketScope: scope,
	}
	propose := a.proposer.ProposeKeyCreate
	if len(scope) > 0 {
		propose = a.proposer.ProposeKeyCreateScoped
	}
	if err := propose(ctx, k); err != nil {
		return KeyCreateResponse{}, &adminapi.Error{Code: "internal", Message: "propose: " + err.Error()}
	}
	return KeyCreateResponse{AccessKey: accessKey, SecretKey: secretKey, SAID: saID, CreatedAt: k.CreatedAt, ExpiresAt: k.ExpiresAt, Buckets: scope}, nil
}

func (a *AdminAPI) HandleKeyCreate(w http.ResponseWriter, r *http.Request, saID string) {
	var req KeyCreateRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}
	resp, err := a.CreateKey(r.Context(), saID, req)
	if err != nil {
		writeAdminError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (a *AdminAPI) RevokeKey(ctx context.Context, saID, accessKey string) error {
	k, ok := a.store.LookupKey(accessKey)
	if !ok || k.SAID != saID {
		return &adminapi.Error{Code: "not_found", Message: "key not found"}
	}
	if err := a.proposer.ProposeKeyRevoke(ctx, accessKey); err != nil {
		return &adminapi.Error{Code: "internal", Message: "propose: " + err.Error()}
	}
	return nil
}

func (a *AdminAPI) HandleKeyRevoke(w http.ResponseWriter, r *http.Request, saID, accessKey string) {
	if err := a.RevokeKey(r.Context(), saID, accessKey); err != nil {
		writeAdminError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// BucketUpstreamPutRequest is the JSON body for PUT /v1/buckets/upstream.
// Both creds are required; secret_key is plaintext on input and wrap-encrypted
// before raft propose. Never echoed back in any response.
//
// JSON wire shape uses `upstream_url` (per /plan-eng-review override A9) so the
// CLI flag --upstream-url and JSON key match.
type BucketUpstreamPutRequest struct {
	Bucket      string `json:"bucket"`
	UpstreamURL string `json:"upstream_url"`
	AccessKey   string `json:"access_key"`
	SecretKey   string `json:"secret_key"`
}

// BucketUpstreamItem is the wire shape for both GET single and GET list.
// SecretKey is intentionally absent — only access_key, upstream_url, and
// metadata leave the server.
type BucketUpstreamItem struct {
	Bucket      string               `json:"bucket"`
	UpstreamURL string               `json:"upstream_url"`
	AccessKey   string               `json:"access_key"`
	CreatedAt   time.Time            `json:"created_at"`
	CreatedBy   string               `json:"created_by,omitempty"`
	Status      BucketUpstreamStatus `json:"status"`
}

func (a *AdminAPI) PutBucketUpstream(ctx context.Context, req BucketUpstreamPutRequest) error {
	if req.Bucket == "" {
		return &adminapi.Error{Code: "invalid", Message: "bucket required"}
	}
	if req.Bucket == "*" || req.Bucket == "__system__" {
		return &adminapi.Error{Code: "invalid", Message: "bucket must not be a sentinel name"}
	}
	if len(req.Bucket) < 3 || len(req.Bucket) > 63 {
		return &adminapi.Error{Code: "invalid", Message: "bucket length must be 3-63 chars"}
	}
	if !validBucketName(req.Bucket) {
		return &adminapi.Error{Code: "invalid", Message: "bucket must match ^[a-z0-9][a-z0-9.-]*[a-z0-9]$"}
	}
	if len(req.UpstreamURL) > 2048 {
		return &adminapi.Error{Code: "invalid", Message: "upstream_url too long (max 2048)"}
	}
	if len(req.AccessKey) > 128 {
		return &adminapi.Error{Code: "invalid", Message: "access_key too long (max 128)"}
	}
	if len(req.SecretKey) > 256 {
		return &adminapi.Error{Code: "invalid", Message: "secret_key too long (max 256)"}
	}
	if req.UpstreamURL == "" {
		return &adminapi.Error{Code: "invalid", Message: "upstream_url required"}
	}
	parsed, err := url.Parse(req.UpstreamURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return &adminapi.Error{Code: "invalid", Message: "upstream_url must be a valid http(s) URL"}
	}
	if req.AccessKey == "" || req.SecretKey == "" {
		return &adminapi.Error{Code: "invalid", Message: "access_key and secret_key required"}
	}
	wrapped, err := WrapSecret(a.enc, "bucket-upstream:"+req.Bucket, req.SecretKey)
	if err != nil {
		return &adminapi.Error{Code: "internal", Message: "wrap secret: " + err.Error()}
	}
	u := BucketUpstream{
		Bucket: req.Bucket, Endpoint: req.UpstreamURL, AccessKey: req.AccessKey,
		SecretKey: req.SecretKey, SecretKeyEnc: wrapped,
		CreatedAt: time.Now().UTC(), CreatedBy: PrincipalFromContext(ctx),
	}
	if err := a.proposer.ProposeBucketUpstreamPut(ctx, u); err != nil {
		return &adminapi.Error{Code: "internal", Message: "propose: " + err.Error()}
	}
	return nil
}

func (a *AdminAPI) HandleBucketUpstreamPut(w http.ResponseWriter, r *http.Request) {
	var req BucketUpstreamPutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.PutBucketUpstream(r.Context(), req); err != nil {
		writeAdminError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *AdminAPI) GetBucketUpstream(_ context.Context, bucket string) (BucketUpstreamItem, error) {
	u, ok := a.store.LookupBucketUpstream(bucket)
	if !ok {
		return BucketUpstreamItem{}, &adminapi.Error{Code: "not_found", Message: "not found"}
	}
	status := u.Status
	if status == "" {
		status = BucketUpstreamStatusActive
	}
	return BucketUpstreamItem{Bucket: u.Bucket, UpstreamURL: u.Endpoint, AccessKey: u.AccessKey, CreatedAt: u.CreatedAt, CreatedBy: u.CreatedBy, Status: status}, nil
}

func (a *AdminAPI) HandleBucketUpstreamGet(w http.ResponseWriter, r *http.Request, bucket string) {
	resp, err := a.GetBucketUpstream(r.Context(), bucket)
	if err != nil {
		writeAdminError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (a *AdminAPI) ListBucketUpstreams(_ context.Context) ([]BucketUpstreamItem, error) {
	st := a.store.snapshot()
	out := make([]BucketUpstreamItem, 0, len(st.bucketUpstreams))
	for _, u := range st.bucketUpstreams {
		status := u.Status
		if status == "" {
			status = BucketUpstreamStatusActive
		}
		out = append(out, BucketUpstreamItem{Bucket: u.Bucket, UpstreamURL: u.Endpoint, AccessKey: u.AccessKey, CreatedAt: u.CreatedAt, CreatedBy: u.CreatedBy, Status: status})
	}
	return out, nil
}

func (a *AdminAPI) HandleBucketUpstreamList(w http.ResponseWriter, r *http.Request) {
	out, _ := a.ListBucketUpstreams(r.Context())
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func (a *AdminAPI) DeleteBucketUpstream(ctx context.Context, bucket string) error {
	if _, ok := a.store.LookupBucketUpstream(bucket); !ok {
		return &adminapi.Error{Code: "not_found", Message: "not found"}
	}
	if err := a.proposer.ProposeBucketUpstreamDelete(ctx, bucket); err != nil {
		return &adminapi.Error{Code: "internal", Message: "propose: " + err.Error()}
	}
	return nil
}

func (a *AdminAPI) HandleBucketUpstreamDelete(w http.ResponseWriter, r *http.Request, bucket string) {
	if err := a.DeleteBucketUpstream(r.Context(), bucket); err != nil {
		writeAdminError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

type BucketUpstreamCutoverRequest struct {
	Bucket string `json:"bucket"`
}

func (a *AdminAPI) CutoverBucketUpstream(ctx context.Context, bucket string) error {
	if bucket == "" {
		return &adminapi.Error{Code: "invalid", Message: "bucket required"}
	}
	if _, ok := a.store.LookupBucketUpstream(bucket); !ok {
		return &adminapi.Error{Code: "not_found", Message: "not found"}
	}
	if err := a.proposer.ProposeBucketUpstreamCutover(ctx, bucket); err != nil {
		var gateErr *compat.GateRejectError
		if errors.As(err, &gateErr) {
			return &adminapi.Error{Code: "conflict", Message: gateErr.PublicMessage()}
		}
		return &adminapi.Error{Code: "internal", Message: "propose cutover: " + err.Error()}
	}
	return nil
}

func (a *AdminAPI) HandleBucketUpstreamCutover(w http.ResponseWriter, r *http.Request) {
	var req BucketUpstreamCutoverRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if err := a.CutoverBucketUpstream(r.Context(), req.Bucket); err != nil {
		writeAdminError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// writeAdminError translates *adminapi.Error to the appropriate HTTP status.
func writeAdminError(w http.ResponseWriter, err error) {
	var ae *adminapi.Error
	if errors.As(err, &ae) {
		status := http.StatusInternalServerError
		switch ae.Code {
		case "not_found":
			status = http.StatusNotFound
		case "invalid":
			status = http.StatusBadRequest
		case "conflict":
			status = http.StatusConflict
		case "forbidden":
			status = http.StatusForbidden
		case "precondition":
			status = http.StatusPreconditionFailed
		}
		http.Error(w, ae.Message, status)
		return
	}
	http.Error(w, err.Error(), http.StatusInternalServerError)
}

// genCredentialPair returns a (access_key, secret_key) pair. AKGF prefix
// distinguishes from AWS "AKIA*" so users don't confuse origins.
func genCredentialPair() (string, string) {
	akBytes := make([]byte, 12)
	skBytes := make([]byte, 30)
	_, _ = rand.Read(akBytes)
	_, _ = rand.Read(skBytes)
	enc := base32.StdEncoding.WithPadding(base32.NoPadding)
	return "AKGF" + strings.ToUpper(enc.EncodeToString(akBytes))[:16],
		enc.EncodeToString(skBytes)
}

// validBucketName checks AWS-compatible bucket naming: lowercase alphanumerics, dots, hyphens.
// First and last char must be alphanumeric. Length is checked by the caller.
func validBucketName(s string) bool {
	if len(s) == 0 {
		return false
	}
	for i, c := range s {
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '.' || c == '-'
		if !ok {
			return false
		}
		if (i == 0 || i == len(s)-1) && (c == '.' || c == '-') {
			return false
		}
	}
	return true
}
