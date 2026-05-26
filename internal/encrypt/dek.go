package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
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
//
// LOCK ORDER: k.mu is always acquired AFTER MetaFSM.mu (f.mu). MetaFSM.Snapshot()
// holds f.mu while calling VersionsAndActive() (which acquires k.mu). KEK rotation
// Apply holds f.mu while calling InstallKEKRotation() (which acquires k.mu).
// Never acquire k.mu before f.mu.
type DEKKeeper struct {
	mu     sync.RWMutex
	kek    []byte
	active uint32
	wrap   map[uint32][]byte      // dek_gen → wrapped DEK (snapshot persistence)
	aead   map[uint32]cipher.AEAD // dek_gen → cached AEAD (hot-path)

	// Seal-count diagnostics for nonce-collision monitoring (Task 13). The
	// count is per active KEK version: every Seal/SealWithAAD/Rewrap under the
	// active AEAD increments activeSeals (a single lock-free atomic.Add — no
	// map lookup on the hot path). On KEK rotation, OnKEKRotation freezes the
	// running count into retiredSeals[old version] and resets activeSeals so
	// the new version starts its own count. activeKEKVersion is the label the
	// counter belongs to (DEK gen and KEK version are distinct: a KEK rotation
	// re-wraps DEKs without bumping the DEK gen).
	activeSeals    atomic.Uint64
	activeKEKVer   atomic.Uint32
	retiredSealsMu sync.RWMutex
	retiredSeals   map[uint32]uint64
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
		kek:          append([]byte(nil), kek...), // defensive copy: caller may zeroize their slice after this returns
		active:       0,
		wrap:         map[uint32][]byte{0: wrapped},
		aead:         map[uint32]cipher.AEAD{0: aead},
		retiredSeals: make(map[uint32]uint64),
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
	k.activeSeals.Add(1)
	return aead.Seal(nonce, nonce, plain, nil), k.active, nil
}

// SealWithAAD encrypts plain using the active generation's cached AEAD,
// binding the ciphertext to aad. The same aad bytes must be supplied to
// OpenWithAAD to decrypt — different aad fails authentication.
func (k *DEKKeeper) SealWithAAD(plain, aad []byte) ([]byte, uint32, error) {
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
	k.activeSeals.Add(1)
	return aead.Seal(nonce, nonce, plain, aad), k.active, nil
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

// OpenWithAAD decrypts ct produced by SealWithAAD under the supplied aad.
func (k *DEKKeeper) OpenWithAAD(ct []byte, gen uint32, aad []byte) ([]byte, error) {
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
	return aead.Open(nil, ct[:ns], ct[ns:], aad)
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
	k.activeSeals.Add(1)
	return activeAEAD.Seal(nonce, nonce, plain, nil), k.active, nil
}

// RewrapWithAAD decrypts ct under the oldGen AEAD with aad and re-seals it
// under the active AEAD with the SAME aad. Used by the rewrap scrubber to
// fold Open+Seal into a single RLock acquisition while preserving the AAD
// binding (so re-wrap does not weaken context binding).
func (k *DEKKeeper) RewrapWithAAD(ct []byte, oldGen uint32, aad []byte) ([]byte, uint32, error) {
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
	plain, err := oldAEAD.Open(nil, ct[:ns], ct[ns:], aad)
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
	k.activeSeals.Add(1)
	return activeAEAD.Seal(nonce, nonce, plain, aad), k.active, nil
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

// SealCount returns the number of active-AEAD seals attributed to KEK version
// kekVersion. For the current active version this is the live running counter;
// for a prior version it is the value frozen by OnKEKRotation at the moment
// that version stopped being active. Unknown versions return 0.
func (k *DEKKeeper) SealCount(kekVersion uint32) uint64 {
	if kekVersion == k.activeKEKVer.Load() {
		return k.activeSeals.Load()
	}
	k.retiredSealsMu.RLock()
	defer k.retiredSealsMu.RUnlock()
	return k.retiredSeals[kekVersion]
}

// ActiveKEKVersion returns the KEK version the seal counter currently
// attributes to. Distinct from the DEK gen returned by Active().
func (k *DEKKeeper) ActiveKEKVersion() uint32 { return k.activeKEKVer.Load() }

// SealCountSnapshot returns every KEK version's seal count: the frozen value
// for each retired version plus the live running count for the current active
// version. Off the hot path (Prometheus scrape via the KEK collector) — reads
// activeKEKVer + activeSeals atomics and the retiredSeals map under its RLock.
func (k *DEKKeeper) SealCountSnapshot() map[uint32]uint64 {
	active := k.activeKEKVer.Load()
	live := k.activeSeals.Load()
	k.retiredSealsMu.RLock()
	out := make(map[uint32]uint64, len(k.retiredSeals)+1)
	for v, c := range k.retiredSeals {
		out[v] = c
	}
	k.retiredSealsMu.RUnlock()
	out[active] = live // active wins over any stale retired entry for the same version
	return out
}

// OnKEKRotation records that the active KEK version changed to newVersion. The
// running seal count is frozen under the previous version and the live counter
// resets to 0 so the new version starts its own count. Called by the FSM
// immediately after InstallKEKRotation under f.mu (single-writer apply path).
func (k *DEKKeeper) OnKEKRotation(newVersion uint32) {
	prev := k.activeKEKVer.Swap(newVersion)
	if prev == newVersion {
		return
	}
	frozen := k.activeSeals.Swap(0)
	k.retiredSealsMu.Lock()
	k.retiredSeals[prev] = frozen
	k.retiredSealsMu.Unlock()
}

// SetActiveKEKVersion sets the KEK version the seal counter attributes to,
// without freezing or resetting the running count. Used at boot
// (wireDEKKeeper / restore) to label the keeper's initial active version,
// which may be > 0 after a snapshot restore.
func (k *DEKKeeper) SetActiveKEKVersion(v uint32) { k.activeKEKVer.Store(v) }

// InstallKEKRotation atomically replaces the keeper's internal KEK and the
// wrapped-DEK map with the post-rotation versions. Used by FSM Apply after
// MetaKEKRotateCmd: the FSM has already verified that every rewrapped wrap
// unwraps to the same plaintext DEK under newKEK as the corresponding old
// wrap under the previous KEK — this method just swaps the references.
//
// Cached AEADs in k.aead are preserved: the underlying DEK plaintexts are
// unchanged across a KEK rotation (only the wrapping changed), so the
// per-gen cipher.AEAD instances remain valid. Pruning a gen still happens
// via Prune().
//
// Defensive copies are taken so the caller (FSM Apply, holding the only
// references to the wire payload bytes) may mutate or zeroize its inputs
// after this returns. Returns an error iff newKEK length is wrong; map
// content is not otherwise validated (callers vet the rewrap set first).
func (k *DEKKeeper) InstallKEKRotation(newKEK []byte, rewrapped map[uint32][]byte) error {
	if len(newKEK) != KEKSize {
		return fmt.Errorf("DEKKeeper.InstallKEKRotation: kek len = %d, want %d", len(newKEK), KEKSize)
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	// Replace KEK in-place over the existing slice when possible (no
	// reallocation if cap is sufficient). The keeper retains a private
	// defensive copy distinct from the caller's slice.
	k.kek = append(k.kek[:0], newKEK...)
	// Replace wrap[] deeply. Drop entries not present in rewrapped — the
	// FSM-side verifier already asserted gen set equality with the payload.
	for g := range k.wrap {
		delete(k.wrap, g)
	}
	for g, w := range rewrapped {
		k.wrap[g] = append([]byte(nil), w...)
	}
	return nil
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
	return &DEKKeeper{
		kek:          append([]byte(nil), kek...), // defensive copy: caller may zeroize their slice after this returns
		active:       active,
		wrap:         wrap,
		aead:         aead,
		retiredSeals: make(map[uint32]uint64),
	}, nil
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
