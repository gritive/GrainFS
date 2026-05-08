package iam

import (
	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

// DefaultSAID is the well-known sa_id used by the admin-UDS-triggered
// first-SA bootstrap. Stable across concurrent proposes so the race
// guard collapses to a single SA via FSM idempotency on (sa_id) and on
// duplicate KeyCreate for the same access_key.
const DefaultSAID = "sa-default"

// ApplyInitFirstSA decodes a composite InitFirstSAPayload and applies
// SA + AccessKey + WildcardGrant. Idempotent: if the first-SA bootstrap
// already completed (SA + active key + wildcard grant all present),
// returns nil. Race guard for concurrent admin UDS callers on an empty
// cluster — both propose, FSM serializes via Raft FIFO, second Apply
// sees committed state and no-ops. Result: store converges to single SA.
//
// Partial-fail recovery: each sub-Apply is independently idempotent, and
// the completeness predicate isFirstSACommitted requires all three
// records. If ApplySACreate succeeds but ApplyKeyCreate fails (e.g.
// transient encrypt/decode error), the next Apply re-fires the missing
// steps; an SA-presence-only check would wrongly skip and leave the
// cluster permanently half-bootstrapped.
func (a *Applier) ApplyInitFirstSA(payload []byte) error {
	if isFirstSACommitted(a.store) {
		return nil
	}
	root := iampb.GetRootAsInitFirstSAPayload(payload, 0)

	if err := a.ApplySACreate(root.SaCreateBlobBytes()); err != nil {
		return err
	}
	if err := a.ApplyKeyCreate(root.KeyCreateBlobBytes()); err != nil {
		return err
	}
	return a.ApplyGrantWildcardPut(root.GrantWildcardBlobBytes())
}

// isFirstSACommitted reports whether the first-SA bootstrap reached its
// terminal state: DefaultSAID exists, owns a wildcard grant, and has at
// least one active key. Used as the idempotency predicate so a
// partially-applied bootstrap (SA committed, key/grant missing) re-fires
// the missing steps on next Apply.
func isFirstSACommitted(s *Store) bool {
	if _, ok := s.LookupSA(DefaultSAID); !ok {
		return false
	}
	st := s.snapshot()
	if _, ok := st.wildcards[DefaultSAID]; !ok {
		return false
	}
	for _, k := range st.keysByAK {
		if k != nil && k.SAID == DefaultSAID && k.Status == KeyStatusActive {
			return true
		}
	}
	return false
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
