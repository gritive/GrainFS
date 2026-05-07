package iam

import (
	"crypto/rand"
	"encoding/base32"
	"encoding/json"
	"net/http"
	"strings"
	"time"

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

	// Sticky auth_enabled — first SA flips the bit. Errors are non-fatal here:
	// the SA + key are already committed; auth-enable is a separate command.
	_ = a.proposer.ProposeAuthEnable(r.Context())

	resp := SACreateResponse{
		SAID:      sa.ID,
		Name:      sa.Name,
		AccessKey: accessKey,
		SecretKey: secretKey,
		CreatedAt: now,
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

func (a *AdminAPI) HandleSAGet(w http.ResponseWriter, r *http.Request, saID string) {
	sa, ok := a.store.LookupSA(saID)
	if !ok {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(sa)
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
type KeyCreateRequest struct {
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

// KeyCreateResponse returns the rotated key with one-time plaintext secret.
type KeyCreateResponse struct {
	AccessKey string     `json:"access_key"`
	SecretKey string     `json:"secret_key"`
	SAID      string     `json:"sa_id"`
	CreatedAt time.Time  `json:"created_at"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
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
	}
	if err := a.proposer.ProposeKeyCreate(r.Context(), k); err != nil {
		http.Error(w, "propose: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(KeyCreateResponse{
		AccessKey: accessKey, SecretKey: secretKey,
		SAID: saID, CreatedAt: k.CreatedAt, ExpiresAt: k.ExpiresAt,
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
