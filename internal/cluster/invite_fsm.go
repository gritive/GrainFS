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
}

// inviteFSM is the deterministic invite registry applied from the Raft log.
// Keyed by invite-id; stores the invite PUBLIC key (private key never reaches
// the server). All mutations come from applied commands so replicas converge.
type inviteFSM struct {
	mu      sync.Mutex
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
	f.mu.Lock()
	defer f.mu.Unlock()
	r, ok := f.records[id]
	if !ok || r.used || now.UnixNano() >= r.expiryNanos {
		return nil, false
	}
	return r.pub, true
}

var errInviteInvalid = errors.New("invite invalid, used, or expired")

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
