package encrypt

import (
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
)

// DEKSize is the size of a Data Encryption Key in bytes (AES-256).
const DEKSize = 32

// ErrDEKGenStillReferenced is returned by Prune when safe=false (rewrap scrub not complete).
var ErrDEKGenStillReferenced = errors.New("DEK generation still referenced by records; rewrap scrub not complete")

// ErrDEKGenUnknown is returned when the requested generation is not present in the keeper.
var ErrDEKGenUnknown = errors.New("DEK generation unknown")

// DEKKeeper holds the cluster-wide DEK generations in memory.
// Each generation maps dek_gen (uint32) to a wrapped (KEK-sealed) DEK.
type DEKKeeper struct {
	mu     sync.RWMutex
	kek    []byte
	active uint32            // current generation
	wrap   map[uint32][]byte // dek_gen → wrapped DEK (sealed by KEK)
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
	wrapped, err := AESGCMSeal(kek, plain)
	if err != nil {
		return nil, err
	}
	return &DEKKeeper{
		kek:    kek,
		active: 0,
		wrap:   map[uint32][]byte{0: wrapped},
	}, nil
}

// Active returns the current active generation number and its wrapped DEK bytes.
func (k *DEKKeeper) Active() (uint32, []byte) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	return k.active, k.wrap[k.active]
}

// unwrapLocked unwraps the DEK for gen. Must be called with at least RLock held.
func (k *DEKKeeper) unwrapLocked(gen uint32) ([]byte, error) {
	w, ok := k.wrap[gen]
	if !ok {
		return nil, ErrDEKGenUnknown
	}
	return AESGCMOpen(k.kek, w)
}

// Seal encrypts plain using the active generation's DEK.
// Returns the ciphertext and the generation number used.
func (k *DEKKeeper) Seal(plain []byte) ([]byte, uint32, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	dek, err := k.unwrapLocked(k.active)
	if err != nil {
		return nil, 0, err
	}
	ct, err := AESGCMSeal(dek, plain)
	if err != nil {
		return nil, 0, err
	}
	return ct, k.active, nil
}

// Open decrypts ct using the DEK for the specified gen.
func (k *DEKKeeper) Open(ct []byte, gen uint32) ([]byte, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()
	dek, err := k.unwrapLocked(gen)
	if err != nil {
		return nil, err
	}
	return AESGCMOpen(dek, ct)
}

// Rotate generates a new DEK at active+1 and makes it the active generation.
func (k *DEKKeeper) Rotate() error {
	k.mu.Lock()
	defer k.mu.Unlock()
	plain := make([]byte, DEKSize)
	if _, err := rand.Read(plain); err != nil {
		return err
	}
	wrapped, err := AESGCMSeal(k.kek, plain)
	if err != nil {
		return err
	}
	k.active++
	k.wrap[k.active] = wrapped
	return nil
}

// Prune drops the wrapped DEK for gen.
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

// LoadFromFSM reconstructs a DEKKeeper from persisted wrapped DEKs (used after raft restore).
// Active gen is set to the maximum key in versions.
//
// Validates len(kek) == KEKSize. A caller that passes a malformed KEK here
// (e.g., a restore pipeline that didn't error-check) would otherwise create a
// keeper that fails every subsequent Seal/Open with no clear root cause.
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
	cp := make(map[uint32][]byte, len(versions))
	for g, w := range versions {
		cp[g] = append([]byte(nil), w...)
	}
	return &DEKKeeper{kek: kek, active: active, wrap: cp}, nil
}
