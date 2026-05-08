package iam

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/encrypt"
)

// DefaultSAID is the well-known sa_id used by the bootstrap shim. Stable
// so follower replays / restart races collide on the same logical SA via
// the FSM Applier's idempotency check.
const DefaultSAID = "sa-default"

// Proposer abstracts the meta-FSM Propose interface so bootstrap and the
// admin API can be unit-tested without raft. Each method must propose the
// corresponding MetaCmd payload through Raft and return only after the
// command has been committed (not just enqueued).
type Proposer interface {
	ProposeSACreate(ctx context.Context, sa ServiceAccount) error
	ProposeSADelete(ctx context.Context, saID string) error
	ProposeKeyCreate(ctx context.Context, k AccessKey) error
	ProposeKeyRevoke(ctx context.Context, accessKey string) error
	ProposeGrantPut(ctx context.Context, g Grant) error
	ProposeGrantDelete(ctx context.Context, saID, bucket string) error
	ProposeGrantWildcardPut(ctx context.Context, g Grant) error
	ProposeGrantWildcardDelete(ctx context.Context, saID string) error
	ProposeAuthEnable(ctx context.Context) error
}

// Bootstrap implements the --access-key/--secret-key shim. It is a no-op
// unless: (1) both flags are present, (2) caller is the cluster leader,
// and (3) bootstrap is not yet "complete" (default SA + active key +
// wildcard grant + sticky auth_enabled bit all present). Idempotent —
// safe to invoke on every node every boot; only the leader proposes the
// missing steps.
//
// Race guard: the FSM apply path is idempotent on (sa_id=DefaultSAID) and
// on duplicate KeyCreate for the same access_key. If a previous Bootstrap
// run committed steps 1..2 but failed before AuthEnable, the next leader
// election re-runs Bootstrap; bootstrapComplete() returns false (sticky
// bit absent), so the missing steps re-fire and converge.
//
// Pre-fix: IsEmpty() short-circuited as soon as ANY SA existed. Leader
// failure between SACreate and AuthEnable left a partial state where
// auth_enabled was false → authzMiddleware skipped IAM layer →
// permissive cluster despite operator-visible "bootstrap success".
func Bootstrap(
	ctx context.Context,
	s *Store,
	p Proposer,
	accessKey, secretKey string,
	isLeader bool,
	enc *encrypt.Encryptor,
) error {
	if accessKey == "" || secretKey == "" {
		return nil // anonymous mode (P9)
	}
	if !isLeader {
		return nil
	}
	if bootstrapComplete(s) {
		return nil
	}

	now := time.Now().UTC()
	sa := ServiceAccount{
		ID:          DefaultSAID,
		Name:        "default",
		Description: "auto-created from --access-key/--secret-key flag",
		CreatedAt:   now,
	}
	if err := p.ProposeSACreate(ctx, sa); err != nil {
		return err
	}

	wrapped, err := WrapSecret(enc, DefaultSAID, secretKey)
	if err != nil {
		return err
	}
	k := AccessKey{
		AccessKey:    accessKey,
		SecretKey:    secretKey,
		SecretKeyEnc: wrapped,
		SAID:         DefaultSAID,
		Status:       KeyStatusActive,
		CreatedAt:    now,
	}
	if err := p.ProposeKeyCreate(ctx, k); err != nil {
		return err
	}

	g := Grant{
		SAID:      DefaultSAID,
		Bucket:    WildcardBucket,
		Role:      RoleAdmin,
		CreatedAt: now,
	}
	if err := p.ProposeGrantWildcardPut(ctx, g); err != nil {
		return err
	}

	return p.ProposeAuthEnable(ctx)
}

// bootstrapComplete reports whether all four bootstrap steps committed:
// default SA exists, default SA has at least one active key, default SA
// has a wildcard grant, and the sticky auth_enabled bit is set. Used by
// Bootstrap to decide whether to re-fire any missing steps; each
// ProposeXxx is idempotent at the FSM apply layer so re-running an
// already-committed step is safe.
func bootstrapComplete(s *Store) bool {
	if _, ok := s.LookupSA(DefaultSAID); !ok {
		return false
	}
	if !s.AuthEnabled() {
		return false
	}
	st := s.snapshot()
	if _, ok := st.wildcards[DefaultSAID]; !ok {
		return false
	}
	hasActiveKey := false
	for _, k := range st.keysByAK {
		if k != nil && k.SAID == DefaultSAID && k.Status == KeyStatusActive {
			hasActiveKey = true
			break
		}
	}
	return hasActiveKey
}

// NewUUIDv7 returns a time-ordered UUID v7 string. Falls back to v4 if
// the v7 entropy source fails (extremely rare; v4 is still unique but
// not time-ordered).
func NewUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}
