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
	ProposeAuthEnable(ctx context.Context) error
}

// Bootstrap implements the --access-key/--secret-key shim. It is a no-op
// unless: (1) both flags are present, (2) caller is the cluster leader,
// and (3) the IAM store is currently empty. Idempotent — safe to invoke
// on every node every boot; only the leader on an empty store proposes.
//
// Race guard: the FSM apply path is idempotent on (sa_id=DefaultSAID) and
// on duplicate KeyCreate for the same access_key, so even if leadership
// transitions mid-bootstrap the resulting state converges.
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
	if !s.IsEmpty() {
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
