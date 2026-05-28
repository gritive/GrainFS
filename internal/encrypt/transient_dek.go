package encrypt

import (
	"crypto/cipher"
	"errors"
	"fmt"
	"sync"
)

// ErrTransientReadOnly is returned by TransientReadOnlyDEK's Open path when
// asked to seal (via the storage.DataEncryptor adapter — encrypt itself has
// no Seal method on this type). The transient view exists only for
// decode-time decryption during MetaFSM.Restore; sealing through it would
// write ciphertext under a stale generation map and silently break gen
// tracking. Callers (or the storage-package adapter) MUST detect this and
// hard-fail the surrounding operation.
var ErrTransientReadOnly = errors.New("encrypt: transient DEK is read-only")

// TransientReadOnlyDEK is a sealed, no-side-effect view over a snapshot's
// DEK version map. It is built during MetaFSM.Restore to decrypt IAM (and
// any future DEK-sealed trailer) BEFORE the live DEKKeeper is rebuilt by
// boot wiring.
//
// Deliberately NOT *DEKKeeper — distinct concrete type prevents accidental
// compile-time promotion into write-path code. There is no Seal, no
// active-gen advancement, no seal-counter bump, and no registration into
// global metrics/registries. AEAD instances are cached lazily on first
// Open per gen; plaintext DEK material is zeroized immediately after the
// AEAD is built (matching LoadFromFSM).
type TransientReadOnlyDEK struct {
	// clusterID is the 16-byte cluster identity bound into each wrap's
	// DomainDEKFSMWrap AAD (same as DEKKeeper.dekWrapAAD).
	clusterID []byte
	versions  map[uint32][]byte // gen → KEK-wrapped DEK ciphertext (defensive copy)
	active    uint32            // active gen (may be 0 — gen 0 is legitimate)
	activeKEK uint32            // KEK version that the wraps are sealed under
	kek       []byte            // unwrap KEK bytes (defensive copy)

	mu    sync.Mutex
	aeads map[uint32]cipher.AEAD // gen → opened AEAD (lazy cache)
}

// NewTransientReadOnlyDEK builds the view. versions[gen] is the wrapped-DEK
// ciphertext; activeKEK identifies the KEK version the wraps are sealed
// under. clusterKEK is consulted ONCE during construction to copy the KEK
// bytes for activeKEK; thereafter the view holds its own copy.
//
// clusterID must be 16 bytes (binding into DomainDEKFSMWrap AAD). versions
// must be non-empty. clusterKEK must contain the requested activeKEK
// version.
func NewTransientReadOnlyDEK(clusterID []byte, versions map[uint32][]byte, active, activeKEK uint32, clusterKEK *KEKStore) (*TransientReadOnlyDEK, error) {
	if len(clusterID) != 16 {
		return nil, fmt.Errorf("encrypt: transient DEK: clusterID must be 16 bytes, got %d", len(clusterID))
	}
	if len(versions) == 0 {
		return nil, fmt.Errorf("encrypt: transient DEK: empty versions")
	}
	if clusterKEK == nil {
		return nil, fmt.Errorf("encrypt: transient DEK: nil clusterKEK")
	}
	kek, err := clusterKEK.Get(activeKEK)
	if err != nil {
		return nil, fmt.Errorf("encrypt: transient DEK: get KEK v%d: %w", activeKEK, err)
	}
	// Defensive copy of versions so the caller cannot mutate state behind our back.
	cp := make(map[uint32][]byte, len(versions))
	for k, v := range versions {
		cp[k] = append([]byte(nil), v...)
	}
	return &TransientReadOnlyDEK{
		clusterID: append([]byte(nil), clusterID...),
		versions:  cp,
		active:    active,
		activeKEK: activeKEK,
		kek:       kek, // KEKStore.Get already returns a fresh copy
		aeads:     make(map[uint32]cipher.AEAD, len(versions)),
	}, nil
}

// ActiveGen returns the active generation recorded in the snapshot. Useful
// for callers that need to confirm a gen is plausible before calling Open.
func (t *TransientReadOnlyDEK) ActiveGen() uint32 { return t.active }

// dekWrapAAD mirrors DEKKeeper.dekWrapAAD: binds the wrap to
// (clusterID, gen, kekVer) under DomainDEKFSMWrap. activeKEK is the version
// the wraps were sealed under at snapshot time.
func (t *TransientReadOnlyDEK) dekWrapAAD(gen uint32) []byte {
	return BuildAAD(DomainDEKFSMWrap, t.clusterID, FieldUint32(gen), FieldUint32(t.activeKEK))
}

// aeadFor returns the cached AEAD for gen, unwrapping + building it on
// first access. The plaintext DEK bytes are zeroized before this method
// returns (defense against memory forensics).
func (t *TransientReadOnlyDEK) aeadFor(gen uint32) (cipher.AEAD, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if a, ok := t.aeads[gen]; ok {
		return a, nil
	}
	wrapped, ok := t.versions[gen]
	if !ok {
		return nil, ErrDEKGenUnknown
	}
	aad := t.dekWrapAAD(gen)
	plain, err := AESGCMOpenWithAAD(t.kek, wrapped, aad)
	if err != nil {
		return nil, fmt.Errorf("encrypt: transient DEK: unwrap gen %d: %w", gen, err)
	}
	a, err := newAEAD(plain)
	zeroize(plain)
	if err != nil {
		return nil, fmt.Errorf("encrypt: transient DEK: build AEAD for gen %d: %w", gen, err)
	}
	t.aeads[gen] = a
	return a, nil
}

// OpenWithAAD decrypts ct under the DEK at gen using the supplied aad.
// Mirrors DEKKeeper.OpenWithAAD so the storage-package adapter can build
// the AAD via BuildAAD(domain, clusterID, fields...) and call this method,
// producing the same plaintext the live DEKKeeperAdapter would have
// produced post-boot.
//
// Read-only: no seal counter, no active-gen advancement, no global state
// mutation.
func (t *TransientReadOnlyDEK) OpenWithAAD(ct []byte, gen uint32, aad []byte) ([]byte, error) {
	aead, err := t.aeadFor(gen)
	if err != nil {
		return nil, err
	}
	ns := aead.NonceSize()
	if len(ct) < ns {
		return nil, ErrCiphertextTooShort
	}
	return aead.Open(nil, ct[:ns], ct[ns:], aad)
}
