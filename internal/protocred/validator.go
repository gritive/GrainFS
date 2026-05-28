package protocred

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"sync"
	"time"
)

const (
	AttachReasonAllowed        = "allowed"
	AttachReasonStaleCompat    = "stale_compat"
	AttachReasonInvalid        = "invalid"
	AttachReasonNotFound       = "not_found"
	AttachReasonSecretMismatch = "secret_mismatch"
	AttachReasonRevoked        = "revoked"
	AttachReasonExpired        = "expired"
	AttachReasonStaleStrict    = "stale_strict"
	AttachReasonSADisabled     = "service_account_disabled"
)

type Validator interface {
	ValidateAttach(ctx context.Context, req AttachRequest) (AttachDecision, error)
}

type AttachRequest struct {
	Protocol        Protocol
	Resource        string
	CredentialID    string
	PresentedSecret string
	RequestedMode   Mode
	Strict          bool
}

type AttachDecision struct {
	Allowed      bool
	Reason       string
	SAID         string
	Mode         Mode
	Stale        bool
	StaleReason  string
	CredentialID string
}

type AttachValidator struct {
	store      *Store
	now        func() time.Time
	allowTTL   time.Duration
	denyTTL    time.Duration
	saDisabled func(string) bool

	mu    sync.Mutex
	cache map[attachCacheKey]attachCacheEntry
}

type ValidatorOption func(*AttachValidator)

func WithValidatorNow(now func() time.Time) ValidatorOption {
	return func(v *AttachValidator) {
		if now != nil {
			v.now = now
		}
	}
}

func WithAttachAllowTTL(ttl time.Duration) ValidatorOption {
	return func(v *AttachValidator) {
		if ttl >= 0 {
			v.allowTTL = ttl
		}
	}
}

func WithAttachDenyTTL(ttl time.Duration) ValidatorOption {
	return func(v *AttachValidator) {
		if ttl >= 0 {
			v.denyTTL = ttl
		}
	}
}

func WithServiceAccountDisabled(fn func(string) bool) ValidatorOption {
	return func(v *AttachValidator) {
		if fn != nil {
			v.saDisabled = fn
		}
	}
}

func NewAttachValidator(store *Store, opts ...ValidatorOption) *AttachValidator {
	if store == nil {
		store = NewStore()
	}
	v := &AttachValidator{
		store:      store,
		now:        func() time.Time { return time.Now().UTC() },
		allowTTL:   30 * time.Second,
		denyTTL:    time.Second,
		saDisabled: func(string) bool { return false },
		cache:      make(map[attachCacheKey]attachCacheEntry),
	}
	for _, opt := range opts {
		opt(v)
	}
	if v.denyTTL > time.Second {
		v.denyTTL = time.Second
	}
	return v
}

func (v *AttachValidator) ValidateAttach(ctx context.Context, req AttachRequest) (AttachDecision, error) {
	if err := ctx.Err(); err != nil {
		return AttachDecision{}, err
	}
	if !validAttachRequest(req) {
		return AttachDecision{Reason: AttachReasonInvalid, CredentialID: req.CredentialID}, ErrInvalid
	}

	now := v.now()
	secretHash := sha256.Sum256([]byte(req.PresentedSecret))
	if req.CredentialID == "" {
		id, err := v.credentialIDForSecret(req, secretHash)
		if err != nil {
			return AttachDecision{Allowed: false, Reason: attachReasonForLookupErr(err)}, err
		}
		req.CredentialID = id
	}
	key := attachCacheKey{
		protocol: req.Protocol,
		resource: req.Resource,
		id:       req.CredentialID,
		secret:   secretHash,
		mode:     req.RequestedMode,
		strict:   req.Strict,
	}
	if decision, ok := v.cachedDecision(key, now); ok {
		return decision, nil
	}

	decision, err := v.evaluate(req, key.secret, now)
	v.storeDecision(key, decision, now)
	return decision, err
}

func (v *AttachValidator) credentialIDForSecret(req AttachRequest, secretHash [sha256.Size]byte) (string, error) {
	items := v.store.list(ListFilter{Protocol: req.Protocol})
	for _, item := range items {
		if subtle.ConstantTimeCompare(secretHash[:], item.SecretHash[:]) != 1 {
			continue
		}
		if item.Resource != req.Resource || item.Mode != req.RequestedMode {
			return "", ErrInvalid
		}
		return item.ID, nil
	}
	return "", ErrNotFound
}

func attachReasonForLookupErr(err error) string {
	switch err {
	case ErrInvalid:
		return AttachReasonInvalid
	case ErrNotFound:
		return AttachReasonNotFound
	default:
		return AttachReasonInvalid
	}
}

func (v *AttachValidator) cachedDecision(key attachCacheKey, now time.Time) (AttachDecision, bool) {
	v.mu.Lock()
	entry, ok := v.cache[key]
	if !ok || !now.Before(entry.expiresAt) {
		if ok {
			delete(v.cache, key)
		}
		v.mu.Unlock()
		return AttachDecision{}, false
	}
	v.mu.Unlock()

	item, ok := v.store.get(key.id)
	if !ok || item.Generation != entry.generation || v.saDisabled(item.SAID) {
		v.mu.Lock()
		delete(v.cache, key)
		v.mu.Unlock()
		return AttachDecision{}, false
	}
	return entry.decision, true
}

func (v *AttachValidator) evaluate(req AttachRequest, secretHash [sha256.Size]byte, now time.Time) (AttachDecision, error) {
	item, ok := v.store.get(req.CredentialID)
	if !ok {
		return AttachDecision{Allowed: false, Reason: AttachReasonNotFound, CredentialID: req.CredentialID}, ErrNotFound
	}
	decision := AttachDecision{
		Reason:       AttachReasonAllowed,
		SAID:         item.SAID,
		Mode:         item.Mode,
		Stale:        item.StaleAt != nil,
		StaleReason:  item.StaleReason,
		CredentialID: item.ID,
	}
	if item.Protocol != req.Protocol || item.Resource != req.Resource || item.Mode != req.RequestedMode {
		decision.Reason = AttachReasonInvalid
		return decision, ErrInvalid
	}
	if subtle.ConstantTimeCompare(secretHash[:], item.SecretHash[:]) != 1 {
		decision.Reason = AttachReasonSecretMismatch
		return decision, ErrInvalid
	}
	if item.RevokedAt != nil {
		decision.Reason = AttachReasonRevoked
		return decision, ErrRevoked
	}
	if item.ExpiresAt != nil && !now.Before(*item.ExpiresAt) {
		decision.Reason = AttachReasonExpired
		return decision, ErrExpired
	}
	if v.saDisabled(item.SAID) {
		decision.Reason = AttachReasonSADisabled
		return decision, ErrRevoked
	}
	if item.StaleAt != nil {
		if req.Strict {
			decision.Reason = AttachReasonStaleStrict
			return decision, ErrRevoked
		}
		decision.Allowed = true
		decision.Reason = AttachReasonStaleCompat
		return decision, nil
	}
	decision.Allowed = true
	return decision, nil
}

func (v *AttachValidator) storeDecision(key attachCacheKey, decision AttachDecision, now time.Time) {
	item, ok := v.store.get(key.id)
	if !ok {
		return
	}
	ttl := v.denyTTL
	if decision.Allowed {
		ttl = v.allowTTL
		if item.ExpiresAt != nil {
			untilExpiry := item.ExpiresAt.Sub(now)
			if untilExpiry < ttl {
				ttl = untilExpiry
			}
		}
	}
	if ttl <= 0 {
		return
	}
	v.mu.Lock()
	v.cache[key] = attachCacheEntry{
		decision:   decision,
		expiresAt:  now.Add(ttl),
		generation: item.Generation,
	}
	v.mu.Unlock()
}

func validAttachRequest(req AttachRequest) bool {
	return req.PresentedSecret != "" &&
		validProtocol(req.Protocol) &&
		validMode(req.RequestedMode) &&
		validResource(req.Resource)
}

type attachCacheKey struct {
	protocol Protocol
	resource string
	id       string
	secret   [sha256.Size]byte
	mode     Mode
	strict   bool
}

type attachCacheEntry struct {
	decision   AttachDecision
	expiresAt  time.Time
	generation uint64
}
