package iam

import (
	"fmt"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

// Applier wraps a Store and decrypts/decodes FlatBuffers IAM payloads, then
// dispatches to Store.apply* methods. It is the single entry point used by
// the meta-FSM apply switch.
type Applier struct {
	store *Store
	enc   *encrypt.Encryptor
}

// NewApplier returns an Applier bound to the given Store and Encryptor.
// Both must be non-nil; the Encryptor decrypts secret_key_enc payloads.
func NewApplier(store *Store, enc *encrypt.Encryptor) *Applier {
	return &Applier{store: store, enc: enc}
}

// Encryptor returns the underlying Encryptor used for decrypting secret_key_enc
// payloads. Read-only accessor — used by MetaFSM snapshot/restore plumbing
// to thread the encryption key into iam.ReadSnapshot without re-plumbing it
// through SetIAM.
func (a *Applier) Encryptor() *encrypt.Encryptor { return a.enc }

// ApplySACreate decodes a SACreatePayload and inserts the SA. Idempotent
// on existing sa_id (returns nil) — guards follower replay and the
// leader-only bootstrap race.
func (a *Applier) ApplySACreate(payload []byte) error {
	p := iampb.GetRootAsSACreatePayload(payload, 0)
	saID := string(p.SaId())
	if saID == "" {
		return fmt.Errorf("iam: SACreate with empty sa_id")
	}
	if _, exists := a.store.LookupSA(saID); exists {
		return nil
	}
	a.store.applySACreate(ServiceAccount{
		ID:          saID,
		Name:        string(p.Name()),
		Description: string(p.Description()),
		CreatedAt:   time.Unix(0, p.CreatedAtUnixNs()),
		CreatedBy:   string(p.CreatedBy()),
	})
	return nil
}

// ApplySADelete removes the SA, cascading its keys, grants, and wildcard.
// The sticky authEnabled bit is intentionally NOT cleared.
func (a *Applier) ApplySADelete(payload []byte) error {
	p := iampb.GetRootAsSADeletePayload(payload, 0)
	saID := string(p.SaId())
	if saID == "" {
		return fmt.Errorf("iam: SADelete with empty sa_id")
	}
	a.store.applySADelete(saID)
	return nil
}

// ApplyKeyCreate decrypts SecretKeyEnc with AAD = sa_id and inserts the
// AccessKey. Wrong AAD or tampered ciphertext returns an error.
// This is the legacy type-23 path; BucketScope is always nil regardless of payload.
func (a *Applier) ApplyKeyCreate(payload []byte) error {
	p := iampb.GetRootAsKeyCreatePayload(payload, 0)
	encBytes := readEncBytes(p)
	return a.applyKeyCreateInternal(
		string(p.SaId()), string(p.AccessKey()),
		encBytes, time.Unix(0, p.CreatedAtUnixNs()), readExpires(p),
		nil, // legacy type 23: scope always nil
	)
}

// ApplyKeyCreateScoped decrypts SecretKeyEnc and inserts the AccessKey with
// bucket_scope enforced. If the scope contains a bucket for which the SA has
// no grant, the apply is a noop (return nil) for raft determinism.
func (a *Applier) ApplyKeyCreateScoped(payload []byte) error {
	p := iampb.GetRootAsKeyCreatePayload(payload, 0)
	encBytes := readEncBytes(p)
	return a.applyKeyCreateInternal(
		string(p.SaId()), string(p.AccessKey()),
		encBytes, time.Unix(0, p.CreatedAtUnixNs()), readExpires(p),
		readScope(p),
	)
}

// applyKeyCreateInternal is the shared implementation for ApplyKeyCreate and
// ApplyKeyCreateScoped. Noops (return nil) on missing SA or scope overgrant
// to keep raft replay deterministic.
func (a *Applier) applyKeyCreateInternal(saID, ak string, encBytes []byte, createdAt time.Time, expires *time.Time, scope []string) error {
	if saID == "" || ak == "" {
		return fmt.Errorf("iam: KeyCreate missing sa_id or access_key")
	}
	// SA must exist; noop on missing to keep raft replay deterministic.
	if _, ok := a.store.LookupSA(saID); !ok {
		log.Warn().Str("sa_id", saID).Str("ak", ak).Msg("iam: KeyCreate apply: SA missing, noop")
		return nil
	}
	// Scope validation: every bucket in scope must have a grant on the SA.
	for _, b := range scope {
		if a.store.LookupGrant(saID, b) == RoleNone {
			log.Warn().Str("sa_id", saID).Str("ak", ak).Str("bucket", b).
				Msg("iam: KeyCreateScoped apply: scope contains bucket without grant, noop (race)")
			return nil
		}
	}
	plain, err := UnwrapSecret(a.enc, saID, encBytes)
	if err != nil {
		return fmt.Errorf("iam: KeyCreate decrypt: %w", err)
	}
	a.store.applyKeyCreate(AccessKey{
		AccessKey:    ak,
		SecretKey:    plain,
		SecretKeyEnc: encBytes,
		SAID:         saID,
		Status:       KeyStatusActive,
		CreatedAt:    createdAt,
		ExpiresAt:    expires,
		BucketScope:  scope,
	})
	return nil
}

func readEncBytes(p *iampb.KeyCreatePayload) []byte {
	n := p.SecretKeyEncLength()
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = byte(p.SecretKeyEnc(i))
	}
	return out
}

func readExpires(p *iampb.KeyCreatePayload) *time.Time {
	if p.ExpiresAtUnixNs() == 0 {
		return nil
	}
	t := time.Unix(0, p.ExpiresAtUnixNs())
	return &t
}

func readScope(p *iampb.KeyCreatePayload) []string {
	n := p.BucketScopeLength()
	if n == 0 {
		return nil
	}
	out := make([]string, n)
	for i := 0; i < n; i++ {
		out[i] = string(p.BucketScope(i))
	}
	return out
}

// ApplyKeyRevoke marks the AccessKey as revoked. Idempotent on
// missing/already-revoked keys.
func (a *Applier) ApplyKeyRevoke(payload []byte) error {
	p := iampb.GetRootAsKeyRevokePayload(payload, 0)
	a.store.applyKeyRevoke(string(p.AccessKey()))
	return nil
}

// ApplyGrantPut inserts or replaces a (sa_id, bucket, role) grant.
// Rejects WildcardBucket — wildcards must use ApplyGrantWildcardPut so
// the admin API cannot accidentally promote a regular SA to superuser.
func (a *Applier) ApplyGrantPut(payload []byte) error {
	p := iampb.GetRootAsGrantPutPayload(payload, 0)
	saID := string(p.SaId())
	bucket := string(p.Bucket())
	if saID == "" || bucket == "" {
		return fmt.Errorf("iam: GrantPut missing sa_id or bucket")
	}
	if bucket == WildcardBucket {
		return fmt.Errorf("iam: GrantPut cannot use wildcard bucket; use GrantWildcardPut")
	}
	a.store.applyGrantPut(Grant{
		SAID:      saID,
		Bucket:    bucket,
		Role:      Role(p.Role()),
		CreatedAt: time.Unix(0, p.CreatedAtUnixNs()),
		CreatedBy: string(p.CreatedBy()),
	})
	return nil
}

// ApplyGrantDelete removes a (sa_id, bucket) grant. Idempotent.
func (a *Applier) ApplyGrantDelete(payload []byte) error {
	p := iampb.GetRootAsGrantDeletePayload(payload, 0)
	a.store.applyGrantDelete(string(p.SaId()), string(p.Bucket()))
	return nil
}

// ApplyGrantWildcardPut sets the wildcard role for an SA. Used by the
// bootstrap shim only; admin API guards regular SAs from this path.
func (a *Applier) ApplyGrantWildcardPut(payload []byte) error {
	p := iampb.GetRootAsGrantWildcardPutPayload(payload, 0)
	saID := string(p.SaId())
	if saID == "" {
		return fmt.Errorf("iam: GrantWildcardPut missing sa_id")
	}
	a.store.applyGrantWildcardPut(Grant{
		SAID:      saID,
		Role:      Role(p.Role()),
		CreatedAt: time.Unix(0, p.CreatedAtUnixNs()),
		CreatedBy: string(p.CreatedBy()),
	})
	return nil
}

// ApplyGrantWildcardDelete removes the wildcard grant for the given SA.
// Idempotent on missing entries.
//
// Lockout invariant: refuse to remove the wildcard from sa-default when
// no explicit per-bucket grants exist. The admin layer (HandleGrantDelete)
// already checks NumExplicitGrants before proposing, but raft is
// concurrent — two admin clients could both pass the read-side guard and
// both propose. Single-applier raft serialization makes the apply check
// race-free: at most one of the two applies sees explicit grants present;
// the second sees the first's mutation reflected in store state.
//
// Determinism: the check reads from the same Store every node mirrors,
// so all replicas reach the same decision. We log + skip-mutation rather
// than return error so historical commits (pre-fix) replay cleanly across
// rolling upgrades and the apply loop's per-entry error path doesn't
// poison consensus telemetry.
func (a *Applier) ApplyGrantWildcardDelete(payload []byte) error {
	p := iampb.GetRootAsGrantWildcardDeletePayload(payload, 0)
	saID := string(p.SaId())
	if saID == "" {
		return fmt.Errorf("iam: GrantWildcardDelete missing sa_id")
	}
	if saID == DefaultSAID && a.store.NumExplicitGrants(saID) == 0 {
		log.Warn().
			Str("sa_id", saID).
			Msg("iam: refusing to apply wildcard delete on sa-default with no explicit grants — would lock cluster out of S3 plane")
		return nil
	}
	a.store.applyGrantWildcardDelete(saID)
	return nil
}

// ApplyAuthEnable flips the sticky auth_enabled bit. Idempotent.
// Payload has no fields; presence in the raft log is the signal.
func (a *Applier) ApplyAuthEnable(_ []byte) error {
	a.store.applyAuthEnable()
	return nil
}
