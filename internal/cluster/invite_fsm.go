package cluster

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"
	"time"
)

type inviteRecord struct {
	pub         ed25519.PublicKey
	expiryNanos int64
	used        bool

	// Pending-redemption binding (Zero-CA two-phase invite-join). Set at
	// Phase-1 to bind a single-use invite to the FIRST (nodeID, SPKI) that
	// redeems it; the invite is only consumed (used=true) at Phase-2 ACK.
	// pendingAtNanos == 0 means "not pending".
	pendingNodeID  string
	pendingSPKI    [32]byte
	pendingAddr    string
	pendingAtNanos int64
}

// inviteFSM is the deterministic invite registry applied from the Raft log.
// Keyed by invite-id; stores the invite PUBLIC key (private key never reaches
// the server). All mutations come from applied commands so replicas converge.
type inviteFSM struct {
	mu      sync.RWMutex
	records map[string]inviteRecord
}

func newInviteFSM() *inviteFSM {
	return &inviteFSM{records: make(map[string]inviteRecord)}
}

func (f *inviteFSM) applyMint(id string, pub ed25519.PublicKey, expiryNanos int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.records[id] = inviteRecord{pub: append(ed25519.PublicKey(nil), pub...), expiryNanos: expiryNanos}
}

func (f *inviteFSM) lookup(id string, now time.Time) (ed25519.PublicKey, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	r, ok := f.records[id]
	if !ok || r.used || now.UnixNano() >= r.expiryNanos {
		return nil, false
	}
	return r.pub, true
}

var errInviteInvalid = errors.New("invite invalid, used, or expired")

// errInvitePendingMismatch is returned when an invite already bound to one
// (nodeID, SPKI) at Phase-1 is re-redeemed by a DIFFERENT identity. The invite
// stays bound to the first redeemer.
var errInvitePendingMismatch = errors.New("invite already pending for a different redeemer")

// applyPending records a Phase-1 pending-redemption binding. The invite must be
// present, not expired (vs nowNanos), and not yet consumed. On first redemption
// it binds the invite to (nodeID, spki, addr). A repeat by the SAME (nodeID,
// spki) is idempotent (no-op success, supports re-retrieval); a repeat by a
// DIFFERENT identity returns errInvitePendingMismatch. nowNanos is supplied by
// the caller (stamped at propose time) — no time.Now() here.
func (f *inviteFSM) applyPending(id string, nodeID string, spki [32]byte, addr string, nowNanos int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.records[id]
	if !ok || r.used || nowNanos >= r.expiryNanos {
		return errInviteInvalid
	}
	if r.pendingAtNanos != 0 {
		// Already pending — only the same redeemer may re-pend.
		if r.pendingNodeID != nodeID || r.pendingSPKI != spki {
			return errInvitePendingMismatch
		}
		return nil
	}
	r.pendingNodeID = nodeID
	r.pendingSPKI = spki
	r.pendingAddr = addr
	r.pendingAtNanos = nowNanos
	f.records[id] = r
	return nil
}

// lookupPending returns the bound (nodeID, spki, addr) when the invite has a
// Phase-1 pending-redemption record; ok is false otherwise.
func (f *inviteFSM) lookupPending(id string) (nodeID string, spki [32]byte, addr string, ok bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	r, present := f.records[id]
	if !present || r.pendingAtNanos == 0 {
		return "", [32]byte{}, "", false
	}
	return r.pendingNodeID, r.pendingSPKI, r.pendingAddr, true
}

func (f *inviteFSM) applyConsume(id string, now time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.records[id]
	if !ok || r.used || now.UnixNano() >= r.expiryNanos {
		return errInviteInvalid
	}
	r.used = true
	f.records[id] = r
	return nil
}

// applyInviteMint decodes a MetaInviteMintCmd and records the invite in the FSM.
func (f *MetaFSM) applyInviteMint(data []byte) error {
	id, pub, expiryNanos, err := decodeInviteMintCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: decode InviteMint: %w", err)
	}
	f.invites.applyMint(id, pub, expiryNanos)
	return nil
}

// applyInviteConsume decodes a MetaInviteConsumeCmd and marks the invite used.
// The consumed_at_nanos field is stamped at propose time (leader clock) so this
// apply path is deterministic — ZERO calls to time.Now() here.
func (f *MetaFSM) applyInviteConsume(data []byte) error {
	id, consumedAtNanos, err := decodeInviteConsumeCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: decode InviteConsume: %w", err)
	}
	if err := f.invites.applyConsume(id, time.Unix(0, consumedAtNanos)); err != nil {
		return fmt.Errorf("meta_fsm: InviteConsume %q: %w", id, err)
	}
	return nil
}

// applyInvitePending decodes a MetaInvitePendingCmd and records the Phase-1
// pending-redemption binding. The pending_at_nanos field is stamped at propose
// time (leader clock) so this apply path is deterministic — ZERO time.Now().
func (f *MetaFSM) applyInvitePending(data []byte) error {
	inviteID, nodeID, spki, addr, pendingAtNanos, err := decodeInvitePendingCmd(data)
	if err != nil {
		return fmt.Errorf("meta_fsm: decode InvitePending: %w", err)
	}
	if err := f.invites.applyPending(inviteID, nodeID, spki, addr, pendingAtNanos); err != nil {
		return fmt.Errorf("meta_fsm: InvitePending %q: %w", inviteID, err)
	}
	return nil
}
