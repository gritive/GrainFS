package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
)

// DEKSize is the size of a Data Encryption Key in bytes (AES-256).
const DEKSize = 32

// ErrDEKGenStillReferenced is returned by Prune when safe=false (rewrap scrub not complete).
var ErrDEKGenStillReferenced = errors.New("DEK generation still referenced by records; rewrap scrub not complete")

// ErrDEKGenUnknown is returned when the requested generation is not present in the keeper.
var ErrDEKGenUnknown = errors.New("DEK generation unknown")

// DEKKeeper holds the cluster-wide DEK generations in memory.
//
// Each generation maps dek_gen (uint32) to two pieces of state:
//   - wrap: the KEK-sealed DEK bytes (persisted in the meta-FSM snapshot)
//   - aead: a cached cipher.AEAD constructed once at insertion time
//
// The hot path (Seal/Open, one call per S3 object I/O) does a single map
// lookup + AEAD operation. It does NOT call aes.NewCipher / cipher.NewGCM
// — those run only on insertion (NewDEKKeeper, Rotate, LoadFromFSM). This
// matches the existing Encryptor pattern in this package and saves ~4 heap
// allocations per object I/O.
//
// Plaintext DEK bytes are zeroed immediately after the AEAD is constructed
// (defense against memory forensics: core dumps, /proc/PID/mem). The
// cipher.AEAD retains the key in opaque crypto state, not in a Go-visible
// slice.
type DEKKeeper struct {
	mu     sync.RWMutex
	kek    []byte
	active uint32
	wrap   map[uint32][]byte      // dek_gen → wrapped DEK (snapshot persistence)
	aead   map[uint32]cipher.AEAD // dek_gen → cached AEAD (hot-path)
}

// NewDEKKeeper creates a new DEKKeeper with generation 0, sealing a freshly
// generated random DEK with kek.
func NewDEKKeeper(kek []byte) (*DEKKeeper, error) {
	if len(kek) != KEKSize {
		return nil, fmt.Errorf("NewDEKKeeper: KEK len = %d, want %d", len(kek), KEKSize)
	}
	plain := make([]byte, DEKSize)
	if _, err := rand.Read(plain); err != nil {
		return nil, err
	}
	defer zeroize(plain)

	wrapped, err := AESGCMSeal(kek, plain)
	if err != nil {
		return nil, err
	}
	aead, err := newAEAD(plain)
	if err != nil {
		return nil, err
	}
	return &DEKKeeper{
		kek:    kek,
		active: 0,
		wrap:   map[uint32][]byte{0: wrapped},
		aead:   map[uint32]cipher.AEAD{0: aead},
	}, nil
}

// Active returns the current active generation number and a copy of its
// wrapped DEK bytes. The copy prevents callers from accidentally mutating
// internal keeper state.
func (k *DEKKeeper) Active() (uint32, []byte) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	w := k.wrap[k.active]
	out := make([]byte, len(w))
	copy(out, w)
	return k.active, out
}

// Seal encrypts plain using the active generation's cached AEAD.
// Returns the ciphertext and the generation number used.
func (k *DEKKeeper) Seal(plain []byte) ([]byte, uint32, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	aead, ok := k.aead[k.active]
	if !ok {
		return nil, 0, ErrDEKGenUnknown
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, 0, err
	}
	return aead.Seal(nonce, nonce, plain, nil), k.active, nil
}

// Open decrypts ct using the cached AEAD for the specified gen.
func (k *DEKKeeper) Open(ct []byte, gen uint32) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	aead, ok := k.aead[gen]
	if !ok {
		return nil, ErrDEKGenUnknown
	}
	ns := aead.NonceSize()
	if len(ct) < ns {
		return nil, ErrCiphertextTooShort
	}
	return aead.Open(nil, ct[:ns], ct[ns:], nil)
}

// Rotate generates a new DEK at active+1 and makes it the active generation.
func (k *DEKKeeper) Rotate() error {
	plain := make([]byte, DEKSize)
	if _, err := rand.Read(plain); err != nil {
		return err
	}
	defer zeroize(plain)

	wrapped, err := AESGCMSeal(k.kek, plain)
	if err != nil {
		return err
	}
	aead, err := newAEAD(plain)
	if err != nil {
		return err
	}

	k.mu.Lock()
	defer k.mu.Unlock()
	k.active++
	k.wrap[k.active] = wrapped
	k.aead[k.active] = aead
	return nil
}

// Prune drops the wrapped DEK and cached AEAD for gen.
// safe=false returns ErrDEKGenStillReferenced (caller skipped rewrap-complete check).
// Pruning the active generation is always refused.
func (k *DEKKeeper) Prune(gen uint32, safe bool) error {
	if !safe {
		return ErrDEKGenStillReferenced
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	if gen == k.active {
		return fmt.Errorf("cannot prune active generation %d", gen)
	}
	delete(k.wrap, gen)
	delete(k.aead, gen)
	return nil
}

// Versions returns a deep copy of the wrapped-DEK map for snapshot persistence.
func (k *DEKKeeper) Versions() map[uint32][]byte {
	k.mu.RLock()
	defer k.mu.RUnlock()
	out := make(map[uint32][]byte, len(k.wrap))
	for g, w := range k.wrap {
		out[g] = append([]byte(nil), w...)
	}
	return out
}

// Rewrap decrypts ct with the DEK for oldGen and reseals it with the active
// DEK in a single RLock acquisition. Used by the rewrap scrubber so each
// record costs one lock + one AEAD-open + one AEAD-seal — half the locking
// of separate Open+Seal calls.
func (k *DEKKeeper) Rewrap(ct []byte, oldGen uint32) ([]byte, uint32, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	oldAEAD, ok := k.aead[oldGen]
	if !ok {
		return nil, 0, ErrDEKGenUnknown
	}
	ns := oldAEAD.NonceSize()
	if len(ct) < ns {
		return nil, 0, ErrCiphertextTooShort
	}
	plain, err := oldAEAD.Open(nil, ct[:ns], ct[ns:], nil)
	if err != nil {
		return nil, 0, err
	}
	defer zeroize(plain)
	activeAEAD, ok := k.aead[k.active]
	if !ok {
		return nil, 0, ErrDEKGenUnknown
	}
	nonce := make([]byte, activeAEAD.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, 0, err
	}
	return activeAEAD.Seal(nonce, nonce, plain, nil), k.active, nil
}

// VersionsAndActive returns a deep copy of the wrapped-DEK map AND the active
// generation under a single RLock acquisition. Snapshot callers MUST use this
// instead of Versions()+Active() — otherwise a Rotate() between the two calls
// produces a snapshot whose `active` references a generation not yet in the
// map (silent FSM inconsistency).
func (k *DEKKeeper) VersionsAndActive() (versions map[uint32][]byte, active uint32) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	out := make(map[uint32][]byte, len(k.wrap))
	for g, w := range k.wrap {
		out[g] = append([]byte(nil), w...)
	}
	return out, k.active
}

// LoadFromFSM reconstructs a DEKKeeper from persisted wrapped DEKs (used after raft restore).
// Active gen is set to the maximum key in versions.
//
// Validates len(kek) == KEKSize. A caller that passes a malformed KEK here
// (e.g., a restore pipeline that didn't error-check) would otherwise create a
// keeper that fails every subsequent Seal/Open with no clear root cause.
//
// Each persisted DEK is unwrapped once here so its AEAD can be cached; the
// plaintext is zeroed before the function returns.
func LoadFromFSM(kek []byte, versions map[uint32][]byte) (*DEKKeeper, error) {
	if len(kek) != KEKSize {
		return nil, fmt.Errorf("LoadFromFSM: KEK len = %d, want %d", len(kek), KEKSize)
	}
	if len(versions) == 0 {
		return nil, errors.New("LoadFromFSM: empty versions map")
	}
	var active uint32
	for g := range versions {
		if g > active {
			active = g
		}
	}
	wrap := make(map[uint32][]byte, len(versions))
	aead := make(map[uint32]cipher.AEAD, len(versions))
	for g, w := range versions {
		wrap[g] = append([]byte(nil), w...)
		plain, err := AESGCMOpen(kek, w)
		if err != nil {
			return nil, fmt.Errorf("LoadFromFSM: unwrap gen %d: %w", g, err)
		}
		a, err := newAEAD(plain)
		zeroize(plain)
		if err != nil {
			return nil, fmt.Errorf("LoadFromFSM: build AEAD for gen %d: %w", g, err)
		}
		aead[g] = a
	}
	return &DEKKeeper{kek: kek, active: active, wrap: wrap, aead: aead}, nil
}

// newAEAD builds an AES-256-GCM AEAD from a 32-byte key.
func newAEAD(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// zeroize overwrites a slice with zeros. Called on plaintext DEK material
// immediately after the AEAD is built so memory forensics (core dumps,
// /proc/PID/mem) does not surface the key.
func zeroize(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
