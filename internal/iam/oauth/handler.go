// Package oauth implements the RFC 6749 §4.4 client_credentials token endpoint
// for the Iceberg REST Catalog.  client_id = S3 access_key, client_secret = S3
// secret_key.  Credential verification is constant-time.  The resulting HS256
// JWT encodes the resolved sa_id (not the access_key) as Sub and the warehouse
// parsed from the PRINCIPAL_ROLE:<warehouse> scope as Warehouse.
package oauth

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
)

// SAResolver resolves an S3 access_key to the owning SA's id and plaintext
// secret_key.  Returns a non-nil error when the key is unknown, revoked, or
// expired — callers must fail closed.
type SAResolver interface {
	// LookupByAccessKey returns (saID, secretKey, nil) or ("", nil, err).
	LookupByAccessKey(ctx context.Context, accessKey string) (saID string, secretKey []byte, err error)
}

// Authorizer gates token issuance: the SA must hold iceberg:GetCatalogConfig
// on the requested warehouse.
type Authorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}

// Handler is an http.Handler that mints Iceberg bearer tokens.
type Handler struct {
	sa    SAResolver
	keys  *iamjwt.KeySet
	authz Authorizer
}

// NewHandler constructs a Handler. All three arguments are required.
func NewHandler(sa SAResolver, keys *iamjwt.KeySet, authz Authorizer) *Handler {
	return &Handler{sa: sa, keys: keys, authz: authz}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "could not parse form body")
		return
	}

	if r.Form.Get("grant_type") != "client_credentials" {
		writeOAuthError(w, http.StatusBadRequest, "unsupported_grant_type", "grant_type must be client_credentials")
		return
	}

	// Credentials: form params take precedence; HTTP Basic is the fallback.
	cid := r.Form.Get("client_id")
	csec := r.Form.Get("client_secret")
	if cid == "" {
		if u, p, ok := r.BasicAuth(); ok {
			cid, csec = u, p
		}
	}
	if cid == "" || csec == "" {
		writeOAuthError(w, http.StatusBadRequest, "invalid_request", "client_id/secret missing")
		return
	}

	// scope must contain exactly one PRINCIPAL_ROLE:<warehouse>.
	warehouse, scopeErr := extractWarehouse(r.Form.Get("scope"))
	if scopeErr != nil {
		writeOAuthError(w, http.StatusBadRequest, "invalid_scope", scopeErr.Error())
		return
	}

	saID, storedSecret, err := h.sa.LookupByAccessKey(r.Context(), cid)
	if err != nil {
		// Run a dummy compare so unknown/revoked access_key and wrong-secret paths
		// consume the same amount of time — prevents access_key enumeration via
		// timing side-channel (F8).
		subtle.ConstantTimeCompare(make([]byte, 32), make([]byte, 32))
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "invalid client credentials")
		return
	}
	if subtle.ConstantTimeCompare(storedSecret, []byte(csec)) != 1 {
		writeOAuthError(w, http.StatusUnauthorized, "invalid_client", "invalid client credentials")
		return
	}

	// Policy gate: SA must hold iceberg:GetCatalogConfig on the warehouse.
	dec := h.authz.Authorize(r.Context(), saID, warehouse, policy.RequestContext{
		Action:   "iceberg:GetCatalogConfig",
		Resource: "arn:aws:s3:::" + warehouse,
		SourceIP: clientIP(r),
	})
	if dec.Decision != policy.DecisionAllow {
		writeOAuthError(w, http.StatusForbidden, "access_denied", "SA cannot access warehouse "+warehouse)
		return
	}

	tok, err := h.keys.Mint(iamjwt.Claims{Sub: saID, Warehouse: warehouse, TTL: 3600 * time.Second})
	if errors.Is(err, iamjwt.ErrNoCurrentKey) {
		if _, genErr := h.keys.GenerateCurrent(); genErr != nil {
			writeOAuthError(w, http.StatusInternalServerError, "server_error", genErr.Error())
			return
		}
		tok, err = h.keys.Mint(iamjwt.Claims{Sub: saID, Warehouse: warehouse, TTL: 3600 * time.Second})
	}
	if err != nil {
		writeOAuthError(w, http.StatusInternalServerError, "server_error", err.Error())
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"access_token": tok,
		"token_type":   "bearer", // lowercase per RFC 6749 §5.1 + DuckDB #18483
		"expires_in":   3600,
	})
}

// extractWarehouse parses exactly one "PRINCIPAL_ROLE:<name>" token from a
// space-delimited scope string.  Returns an error if zero or more than one
// PRINCIPAL_ROLE token is present.  The warehouse name must be a plain
// identifier: non-empty, no slashes, and no ".." sequences.
func extractWarehouse(scope string) (string, error) {
	var found []string
	for _, part := range strings.Fields(scope) {
		if strings.HasPrefix(part, "PRINCIPAL_ROLE:") {
			found = append(found, strings.TrimPrefix(part, "PRINCIPAL_ROLE:"))
		}
	}
	switch len(found) {
	case 1:
		name := found[0]
		if name == "" {
			return "", fmt.Errorf("scope PRINCIPAL_ROLE: must specify a non-empty warehouse")
		}
		if strings.Contains(name, "/") {
			return "", fmt.Errorf("warehouse name must be a plain identifier, not a URI or path")
		}
		if strings.Contains(name, "..") {
			return "", fmt.Errorf("warehouse name must not contain '..'")
		}
		return name, nil
	case 0:
		return "", fmt.Errorf("scope must include PRINCIPAL_ROLE:<warehouse>")
	default:
		return "", fmt.Errorf("scope must contain exactly one PRINCIPAL_ROLE:<warehouse> token")
	}
}

func clientIP(r *http.Request) string {
	remote := strings.TrimSpace(r.RemoteAddr)
	if host, _, err := net.SplitHostPort(remote); err == nil {
		return host
	}
	if strings.HasPrefix(remote, "[") {
		if end := strings.IndexByte(remote, ']'); end > 0 {
			return remote[1:end]
		}
	}
	return remote
}

func writeOAuthError(w http.ResponseWriter, code int, errKind, desc string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"error":             errKind,
		"error_description": desc,
	})
}
