package receipt

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

// Signing-related sentinel errors. Callers should compare with errors.Is;
// scrubber repair paths must treat ErrNoActiveKey as fail-fast per the Phase 16
// Week 5 failure policy — no unsigned receipts are ever produced.
var (
	ErrNoActiveKey       = errors.New("receipt: no active signing key")
	ErrUnknownKey        = errors.New("receipt: unknown key id")
	ErrSignatureMismatch = errors.New("receipt: signature mismatch")
	ErrPayloadMismatch   = errors.New("receipt: canonical payload mismatch")
)

// Sign canonicalizes r, computes HMAC-SHA256 with the active key, and writes
// the results into r.KeyID, r.CanonicalPayload, and r.Signature. Any existing
// values in those three fields are overwritten.
func Sign(r *HealReceipt, ks *KeyStore) error {
	if ks == nil {
		return ErrNoActiveKey
	}
	active, ok := ks.Active()
	if !ok {
		return ErrNoActiveKey
	}

	r.KeyID = active.ID
	payload, err := canonicalize(r)
	if err != nil {
		return fmt.Errorf("receipt: canonicalize: %w", err)
	}

	mac := hmac.New(sha256.New, active.Secret)
	mac.Write(payload)
	sig := hex.EncodeToString(mac.Sum(nil))

	r.CanonicalPayload = string(payload)
	r.Signature = sig
	return nil
}

// Verify recomputes the signature of r using the key identified by r.KeyID
// and returns nil when it matches r.Signature. Unknown key IDs return
// ErrUnknownKey; mismatched signatures return ErrSignatureMismatch.
func Verify(r *HealReceipt, ks *KeyStore) error {
	if ks == nil {
		return ErrUnknownKey
	}
	key, ok := ks.Lookup(r.KeyID)
	if !ok {
		return fmt.Errorf("%w: %q", ErrUnknownKey, r.KeyID)
	}

	payload, err := canonicalize(r)
	if err != nil {
		return fmt.Errorf("receipt: canonicalize: %w", err)
	}

	// If CanonicalPayload is populated, it must match the re-canonicalized form.
	// This closes an audit gap: without this check, tampering with the stored
	// CanonicalPayload string would leave a misleading record even though the
	// signature still verifies against the fields.
	if r.CanonicalPayload != "" && r.CanonicalPayload != string(payload) {
		return ErrPayloadMismatch
	}

	want, err := hex.DecodeString(r.Signature)
	if err != nil {
		return fmt.Errorf("%w: signature not hex: %v", ErrSignatureMismatch, err)
	}

	mac := hmac.New(sha256.New, key.Secret)
	mac.Write(payload)
	got := mac.Sum(nil)

	if !hmac.Equal(got, want) {
		return ErrSignatureMismatch
	}
	return nil
}
