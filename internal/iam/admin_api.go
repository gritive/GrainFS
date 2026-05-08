package iam

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/rs/zerolog/log"

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
//
// Warning, when present, signals a non-fatal post-create step failed —
// e.g. ProposeAuthEnable for the first SA could not commit. The SA + key
// are already committed and useful, but the cluster may not yet enforce
// auth. Operators should investigate and (if needed) re-run admin steps.
type SACreateResponse struct {
	SAID      string    `json:"sa_id"`
	Name      string    `json:"name"`
	AccessKey string    `json:"access_key"`
	SecretKey string    `json:"secret_key"`
	CreatedAt time.Time `json:"created_at"`
	Warning   string    `json:"warning,omitempty"`
}

type SAListItem struct {
	SAID        string    `json:"sa_id"`
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	NumKeys     int       `json:"num_keys"`
	NumGrants   int       `json:"num_grants"`
}

// AdminAPI hosts HTTP handlers for /admin/iam/* endpoints. Stdlib handlers
// are wrapped onto Hertz at the admin UDS in Task 21.
type AdminAPI struct {
	store    *Store
	proposer Proposer
	enc      *encrypt.Encryptor
}

func NewAdminAPI(store *Store, proposer Proposer, enc *encrypt.Encryptor) *AdminAPI {
	return &AdminAPI{store: store, proposer: proposer, enc: enc}
}

func (a *AdminAPI) HandleSACreate(w http.ResponseWriter, r *http.Request) {
	var req SACreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.Name == "" {
		http.Error(w, "name required", http.StatusBadRequest)
		return
	}

	now := time.Now().UTC()
	sa := ServiceAccount{
		ID:          NewUUIDv7(),
		Name:        req.Name,
		Description: req.Description,
		CreatedAt:   now,
		CreatedBy:   PrincipalFromContext(r.Context()),
	}
	if err := a.proposer.ProposeSACreate(r.Context(), sa); err != nil {
		http.Error(w, "propose SA: "+err.Error(), http.StatusInternalServerError)
		return
	}

	accessKey, secretKey := genCredentialPair()
	wrapped, err := WrapSecret(a.enc, sa.ID, secretKey)
	if err != nil {
		http.Error(w, "wrap secret: "+err.Error(), http.StatusInternalServerError)
		return
	}
	k := AccessKey{
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SecretKeyEnc: wrapped,
		SAID:         sa.ID,
		Status:       KeyStatusActive,
		CreatedAt:    now,
	}
	if err := a.proposer.ProposeKeyCreate(r.Context(), k); err != nil {
		http.Error(w, "propose key: "+err.Error(), http.StatusInternalServerError)
		return
	}

	// Sticky auth_enabled — first SA flips the bit. The SA + key are
	// already committed; an AuthEnable failure here is non-fatal but must
	// be surfaced so operators don't assume a permissive-by-default cluster
	// is enforcing IAM. Pre-fix swallowed this error with `_ =`.
	var warning string
	if err := a.proposer.ProposeAuthEnable(r.Context()); err != nil {
		log.Warn().
			Err(err).
			Str("sa_id", sa.ID).
			Msg("iam: ProposeAuthEnable failed after SA create; sticky bit may not be set — re-run after cluster recovers")
		warning = "AuthEnable failed: cluster may not enforce auth until next bootstrap. Check audit logs."
	}

	resp := SACreateResponse{
		SAID:      sa.ID,
		Name:      sa.Name,
		AccessKey: accessKey,
		SecretKey: secretKey,
		CreatedAt: now,
		Warning:   warning,
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (a *AdminAPI) HandleSAList(w http.ResponseWriter, r *http.Request) {
	st := a.store.snapshot()
	out := make([]SAListItem, 0, len(st.sas))
	for id, sa := range st.sas {
		nKeys := 0
		for _, k := range st.keysByAK {
			if k.SAID == id {
				nKeys++
			}
		}
		nGrants := 0
		if per, ok := st.grants[id]; ok {
			nGrants = len(per)
		}
		if _, ok := st.wildcards[id]; ok {
			nGrants++
		}
		out = append(out, SAListItem{
			SAID: sa.ID, Name: sa.Name, Description: sa.Description,
			CreatedAt: sa.CreatedAt, NumKeys: nKeys, NumGrants: nGrants,
		})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
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

func (a *AdminAPI) HandleSAGet(w http.ResponseWriter, r *http.Request, saID string) {
	sa, ok := a.store.LookupSA(saID)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(SAGetResponse{
		SAID:        sa.ID,
		Name:        sa.Name,
		Description: sa.Description,
		CreatedAt:   sa.CreatedAt,
		CreatedBy:   sa.CreatedBy,
	})
}

func (a *AdminAPI) HandleSADelete(w http.ResponseWriter, r *http.Request, saID string) {
	if _, ok := a.store.LookupSA(saID); !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := a.proposer.ProposeSADelete(r.Context(), saID); err != nil {
		http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
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

func (a *AdminAPI) HandleKeyCreate(w http.ResponseWriter, r *http.Request, saID string) {
	if _, ok := a.store.LookupSA(saID); !ok {
		http.Error(w, "SA not found", http.StatusNotFound)
		return
	}
	var req KeyCreateRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req) // empty body OK
	}

	scope, err := NormalizeScope(req.Buckets)
	if err != nil {
		http.Error(w, err.Error(), http.StatusUnprocessableEntity)
		return
	}
	for _, b := range scope {
		if a.store.LookupGrant(saID, b) == RoleNone {
			http.Error(w, fmt.Sprintf("scope contains %q but SA has no grant on it", b), http.StatusUnprocessableEntity)
			return
		}
	}

	accessKey, secretKey := genCredentialPair()
	wrapped, err := WrapSecret(a.enc, saID, secretKey)
	if err != nil {
		http.Error(w, "wrap: "+err.Error(), http.StatusInternalServerError)
		return
	}
	k := AccessKey{
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SecretKeyEnc: wrapped,
		SAID:         saID,
		Status:       KeyStatusActive,
		CreatedAt:    time.Now().UTC(),
		ExpiresAt:    req.ExpiresAt,
		BucketScope:  scope,
	}
	if len(scope) > 0 {
		if err := a.proposer.ProposeKeyCreateScoped(r.Context(), k); err != nil {
			http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
			return
		}
	} else {
		if err := a.proposer.ProposeKeyCreate(r.Context(), k); err != nil {
			http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(KeyCreateResponse{
		AccessKey: accessKey, SecretKey: secretKey,
		SAID: saID, CreatedAt: k.CreatedAt, ExpiresAt: k.ExpiresAt,
		Buckets: scope,
	})
}

func (a *AdminAPI) HandleKeyRevoke(w http.ResponseWriter, r *http.Request, saID, accessKey string) {
	k, ok := a.store.LookupKey(accessKey)
	if !ok || k.SAID != saID {
		http.Error(w, "key not found", http.StatusNotFound)
		return
	}
	if err := a.proposer.ProposeKeyRevoke(r.Context(), accessKey); err != nil {
		http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
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

type GrantListItem struct {
	SAID   string `json:"sa_id"`
	Bucket string `json:"bucket"`
	Role   string `json:"role"`
}

func (a *AdminAPI) HandleGrantPut(w http.ResponseWriter, r *http.Request) {
	var req GrantPutRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.SAID == "" || req.Bucket == "" || req.Role == "" {
		http.Error(w, "sa_id, bucket, role required", http.StatusBadRequest)
		return
	}
	if req.Bucket == WildcardBucket {
		// P3 guard — wildcard grants are reserved for the bootstrap default SA.
		http.Error(w, "wildcard grant is reserved for bootstrap default SA only", http.StatusForbidden)
		return
	}
	role, err := parseRoleString(req.Role)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, ok := a.store.LookupSA(req.SAID); !ok {
		http.Error(w, "SA not found", http.StatusNotFound)
		return
	}
	g := Grant{
		SAID: req.SAID, Bucket: req.Bucket, Role: role,
		CreatedAt: time.Now().UTC(),
		CreatedBy: PrincipalFromContext(r.Context()),
	}
	if err := a.proposer.ProposeGrantPut(r.Context(), g); err != nil {
		http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (a *AdminAPI) HandleGrantDelete(w http.ResponseWriter, r *http.Request) {
	var req GrantDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.SAID == "" || req.Bucket == "" {
		http.Error(w, "sa_id and bucket required", http.StatusBadRequest)
		return
	}
	if req.Bucket == WildcardBucket {
		// Footgun guard: refuse if removing this would leave sa-default with
		// zero grants while auth_enabled is sticky-on. Operators who want to
		// downgrade sa-default to per-bucket access must FIRST issue at least
		// one explicit grant (e.g., via CreateBucket or admin grant put), then
		// remove the wildcard.
		if req.SAID == DefaultSAID && a.store.NumExplicitGrants(req.SAID) == 0 {
			http.Error(w, "refusing to remove wildcard from sa-default with no explicit grants — issue at least one explicit grant first to avoid lockout", http.StatusConflict)
			return
		}
		if err := a.proposer.ProposeGrantWildcardDelete(r.Context(), req.SAID); err != nil {
			http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err := a.proposer.ProposeGrantDelete(r.Context(), req.SAID, req.Bucket); err != nil {
		http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleGrantList serves both ?sa= and ?bucket= filters. Linear scan over the
// in-memory state — sub-ms up to ~1000 SAs per design doc.
func (a *AdminAPI) HandleGrantList(w http.ResponseWriter, r *http.Request) {
	saFilter := r.URL.Query().Get("sa")
	bucketFilter := r.URL.Query().Get("bucket")
	st := a.store.snapshot()
	out := make([]GrantListItem, 0)
	for saID, per := range st.grants {
		if saFilter != "" && saFilter != saID {
			continue
		}
		for bucket, role := range per {
			if bucketFilter != "" && bucketFilter != bucket {
				continue
			}
			out = append(out, GrantListItem{SAID: saID, Bucket: bucket, Role: role.String()})
		}
	}
	if bucketFilter == "" {
		for saID, role := range st.wildcards {
			if saFilter != "" && saFilter != saID {
				continue
			}
			out = append(out, GrantListItem{SAID: saID, Bucket: WildcardBucket, Role: role.String()})
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

func parseRoleString(s string) (Role, error) {
	switch s {
	case "Read":
		return RoleRead, nil
	case "Write":
		return RoleWrite, nil
	case "Admin":
		return RoleAdmin, nil
	default:
		return RoleNone, fmt.Errorf("invalid role %q (want Read|Write|Admin)", s)
	}
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
