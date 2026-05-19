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

// ApplySADelete removes the SA, cascading its keys.
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
// bucket_scope enforced.
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
// ApplyKeyCreateScoped. Noops (return nil) on missing SA to keep raft replay
// deterministic.
func (a *Applier) applyKeyCreateInternal(saID, ak string, encBytes []byte, createdAt time.Time, expires *time.Time, scope []string) error {
	if saID == "" || ak == "" {
		return fmt.Errorf("iam: KeyCreate missing sa_id or access_key")
	}
	// SA must exist; noop on missing to keep raft replay deterministic.
	if _, ok := a.store.LookupSA(saID); !ok {
		log.Warn().Str("sa_id", saID).Str("ak", ak).Bool("scoped", len(scope) > 0).
			Msg("iam: applyKeyCreateInternal: SA missing, noop")
		return nil
	}
	// Defensive: re-validate scope shape (sentinels, empty entries, dedup).
	if len(scope) > 0 {
		normalized, err := NormalizeScope(scope)
		if err != nil {
			log.Warn().Str("sa_id", saID).Str("ak", ak).Err(err).
				Msg("iam: applyKeyCreateInternal: scope validation failed, noop")
			return nil
		}
		scope = normalized
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

// applyKeyCreateFromSnapshot persists an AccessKey during snapshot restore.
// Unlike applyKeyCreateInternal, it skips scope validation because snapshot
// restore reads SA → Keys in order.
// Validation already happened at issue time; persisted state is trusted.
func (a *Applier) applyKeyCreateFromSnapshot(saID, ak string, encBytes []byte, createdAt time.Time, expires *time.Time, scope []string) error {
	if saID == "" || ak == "" {
		return fmt.Errorf("iam: snapshot restore: KeyCreate missing sa_id or access_key")
	}
	plain, err := UnwrapSecret(a.enc, saID, encBytes)
	if err != nil {
		return fmt.Errorf("iam: snapshot restore: KeyCreate decrypt: %w", err)
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

// ApplyBucketUpstreamPut decodes a BucketUpstreamPutPayload and stores the
// upstream record. SecretKeyEnc is decrypted with AAD = "bucket-upstream:"+bucket
// (per /plan-eng-review override A2 — namespace-prefixed AAD provably disjoint
// from sa_id AAD space). Resulting plaintext SecretKey is held in-memory only
// and never persisted.
func (a *Applier) ApplyBucketUpstreamPut(payload []byte) error {
	p := iampb.GetRootAsBucketUpstreamPutPayload(payload, 0)
	bucket := string(p.Bucket())
	if bucket == "" {
		return fmt.Errorf("iam: BucketUpstreamPut missing bucket")
	}
	if bucket == "*" || bucket == "__system__" {
		return fmt.Errorf("iam: BucketUpstreamPut rejects sentinel bucket %q", bucket)
	}
	// Defense-in-depth: admin handler enforces this too, but a direct raft
	// inject must not bypass the hard length bounds.
	if len(bucket) < 3 || len(bucket) > 63 {
		return fmt.Errorf("iam: BucketUpstreamPut bucket length out of range (3-63), got %d", len(bucket))
	}
	encBytes := readBucketUpstreamEncBytes(p)
	plain, err := UnwrapSecret(a.enc, "bucket-upstream:"+bucket, encBytes)
	if err != nil {
		return fmt.Errorf("iam: BucketUpstreamPut decrypt: %w", err)
	}
	status := BucketUpstreamStatus(string(p.Status()))
	if status == "" {
		status = BucketUpstreamStatusActive
	}
	if status != BucketUpstreamStatusActive && status != BucketUpstreamStatusCutover {
		return fmt.Errorf("iam: BucketUpstreamPut invalid status %q", status)
	}
	a.store.applyBucketUpstreamPut(BucketUpstream{
		Bucket:       bucket,
		Endpoint:     string(p.Endpoint()),
		AccessKey:    string(p.AccessKey()),
		SecretKey:    plain,
		SecretKeyEnc: encBytes,
		CreatedAt:    time.Unix(0, p.CreatedAtUnixNs()),
		CreatedBy:    string(p.CreatedBy()),
		Status:       status,
	})
	return nil
}

func (a *Applier) ApplyBucketUpstreamStatusSet(bucket string, status BucketUpstreamStatus) error {
	if bucket == "" {
		return fmt.Errorf("iam: BucketUpstreamStatusSet missing bucket")
	}
	if status != BucketUpstreamStatusActive && status != BucketUpstreamStatusCutover {
		return fmt.Errorf("iam: BucketUpstreamStatusSet invalid status %q", status)
	}
	u, ok := a.store.LookupBucketUpstream(bucket)
	if !ok {
		return fmt.Errorf("iam: BucketUpstreamStatusSet upstream %q not found", bucket)
	}
	u.Status = status
	a.store.applyBucketUpstreamPut(*u)
	return nil
}

// ApplyBucketUpstreamDelete removes the upstream record for the given bucket.
// Idempotent on missing buckets.
func (a *Applier) ApplyBucketUpstreamDelete(payload []byte) error {
	p := iampb.GetRootAsBucketUpstreamDeletePayload(payload, 0)
	a.store.applyBucketUpstreamDelete(string(p.Bucket()))
	return nil
}
