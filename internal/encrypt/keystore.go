package encrypt

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
)

// ErrKEKVersionUnknown is returned by Get/Delete when the requested version
// is not present in the store.
var ErrKEKVersionUnknown = errors.New("KEK version unknown")

// ErrKEKVersionDuplicate is returned by Add when the version is already
// present. Adding the same version twice is a programmer error — the caller
// should Get-then-update if intentional replacement is required.
var ErrKEKVersionDuplicate = errors.New("KEK version already present")

// ErrKEKActiveInUse is returned by Delete when the caller attempts to remove
// the active version. The active version must be advanced via Add(higher)
// before the old version can be deleted.
var ErrKEKActiveInUse = errors.New("cannot delete active KEK version")

// KEKStore holds multiple KEK versions in memory. The highest-numbered
// version is the active one (the one used to wrap freshly-generated DEKs).
//
// Concurrency: a sync.RWMutex guards the keys map. KEK operations (Add,
// Delete) are admin-rate, not hot-path — a Mutex would suffice, but Get
// runs on the hot path during DEK operations, hence RWMutex.
type KEKStore struct {
	mu               sync.RWMutex
	keys             map[uint32][]byte
	active           uint32
	retiringVersions map[uint32]struct{}
}

// NewKEKStore creates an empty KEKStore.
func NewKEKStore() *KEKStore {
	return &KEKStore{
		keys:             make(map[uint32][]byte),
		retiringVersions: make(map[uint32]struct{}),
	}
}

// Retire marks the given version as retiring in memory. The key bytes are
// retained until a subsequent MetaKEKPruneCmd removes them. Returns an error
// if the version is not loaded.
func (s *KEKStore) Retire(version uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[version]; !ok {
		return fmt.Errorf("KEKStore.Retire: version %d not loaded", version)
	}
	s.retiringVersions[version] = struct{}{}
	return nil
}

// IsRetiring reports whether the given version has been marked as retiring.
func (s *KEKStore) IsRetiring(version uint32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.retiringVersions[version]
	return ok
}

// Add inserts kek at version. Refuses len(kek) != KEKSize and duplicate
// versions. Advancing the active version is a side effect when version >
// current active.
func (s *KEKStore) Add(version uint32, kek []byte) error {
	if len(kek) != KEKSize {
		return fmt.Errorf("KEKStore.Add: kek len = %d, want %d", len(kek), KEKSize)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, dup := s.keys[version]; dup {
		return ErrKEKVersionDuplicate
	}
	stored := append([]byte(nil), kek...)
	s.keys[version] = stored
	if len(s.keys) == 1 || version > s.active {
		s.active = version
	}
	return nil
}

// Get returns a copy of the KEK at version. Copy prevents callers from
// mutating internal state.
func (s *KEKStore) Get(version uint32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	k, ok := s.keys[version]
	if !ok {
		return nil, fmt.Errorf("%w: %d", ErrKEKVersionUnknown, version)
	}
	out := make([]byte, KEKSize)
	copy(out, k)
	return out, nil
}

// ActiveVersion returns the current active KEK version. Returns 0 on an
// empty store (caller must check Versions() if 0-vs-empty matters).
func (s *KEKStore) ActiveVersion() uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.active
}

// HasVersion reports whether the given version is loaded in the store.
// Read-locked; safe for concurrent use with hot-path Get.
func (s *KEKStore) HasVersion(version uint32) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	_, ok := s.keys[version]
	return ok
}

// SetActiveVersion overrides the active KEK marker to the requested version.
// Fails if the version is not loaded. Used by MetaKEKRotateCmd FSM Apply to
// flip the active marker atomically with the in-memory + on-disk install of
// the rewrapped DEK set (Phase B). Distinct from Add, which only advances
// active when the new version is strictly greater.
func (s *KEKStore) SetActiveVersion(version uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.keys[version]; !ok {
		return fmt.Errorf("KEKStore.SetActiveVersion: version %d not loaded", version)
	}
	s.active = version
	return nil
}

// ActiveKEK returns the KEK bytes for the active version. Convenience over
// Get(ActiveVersion()).
func (s *KEKStore) ActiveKEK() ([]byte, error) {
	s.mu.RLock()
	v := s.active
	s.mu.RUnlock()
	return s.Get(v)
}

// Versions returns all stored versions in ascending order. Returns a fresh
// slice — caller may mutate.
func (s *KEKStore) Versions() []uint32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]uint32, 0, len(s.keys))
	for v := range s.keys {
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// Delete removes the KEK at version. Refuses deletion of the active
// version (must advance active first). Refuses unknown versions. Zeroizes
// in-memory bytes before unlinking from the map.
func (s *KEKStore) Delete(version uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if version == s.active {
		return ErrKEKActiveInUse
	}
	bytes, ok := s.keys[version]
	if !ok {
		return fmt.Errorf("%w: %d", ErrKEKVersionUnknown, version)
	}
	for i := range bytes {
		bytes[i] = 0
	}
	delete(s.keys, version)
	return nil
}

// ErrLegacyKEKDetected indicates the on-disk layout still has the pre-v1
// single-file kek.key alongside the new keys/ directory. The caller MUST
// refuse boot — silently migrating would conflict with the green-field
// cutover invariants of a later phase.
var ErrLegacyKEKDetected = errors.New("legacy kek.key file detected; refusing to boot in Phase A. Green-field cutover required.")

// parseKeyFilename accepts "<uint32>.key" and rejects everything else.
// Centralised so the parser stays consistent with the writer.
func parseKeyFilename(name string) (uint32, bool) {
	if !strings.HasSuffix(name, ".key") {
		return 0, false
	}
	stem := strings.TrimSuffix(name, ".key")
	v, err := strconv.ParseUint(stem, 10, 32)
	if err != nil {
		return 0, false
	}
	// Reject non-canonical forms: leading zeros, "+1", etc.
	if strconv.FormatUint(v, 10) != stem {
		return 0, false
	}
	return uint32(v), true
}

// KeysDirIsEmpty reports whether keysDir contains zero KEK files in the
// canonical `<uint32>.key` form. Missing directory counts as empty.
// Files with the wrong filename pattern (e.g. backups, operator notes)
// are ignored — they do not make the dir "non-empty" for boot-mode
// auto-generate gating, because LoadOrInitKEKStoreDir would skip them
// the same way.
func KeysDirIsEmpty(keysDir string) (bool, error) {
	entries, err := os.ReadDir(keysDir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return true, nil
		}
		return false, fmt.Errorf("KeysDirIsEmpty: %w", err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if _, ok := parseKeyFilename(e.Name()); ok {
			return false, nil
		}
	}
	return true, nil
}

// LoadOrInitKEKStoreDir loads all `<V>.key` files from keysDir into a fresh
// KEKStore. If the directory is missing or empty, it generates a fresh
// version 0 KEK and persists it. Refuses boot if a legacy `kek.key` exists
// at the sibling path (parent of keysDir), to prevent silent migration
// ambiguity.
//
// File invariants:
//   - Files are exactly KEKSize bytes (32).
//   - Mode must be 0o600 (operator's responsibility to maintain).
//   - No symlinks (O_NOFOLLOW).
func LoadOrInitKEKStoreDir(keysDir string) (*KEKStore, error) {
	parent := filepath.Dir(keysDir)
	legacy := filepath.Join(parent, "kek.key")
	if _, err := os.Lstat(legacy); err == nil {
		return nil, fmt.Errorf("%w: %s", ErrLegacyKEKDetected, legacy)
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("KEKStore: stat legacy %q: %w", legacy, err)
	}

	// Durably create the keys/ directory if absent so the new directory
	// entry survives a crash. POSIX requires fsync(parent_of_dir) to make a
	// new directory entry durable; MkdirAll alone is not enough. If the dir
	// already exists (operator pre-created it, or a prior boot), skip the
	// extra fsync.
	if _, err := os.Stat(keysDir); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("KEKStore: stat %q: %w", keysDir, err)
		}
		if err := os.MkdirAll(keysDir, 0o700); err != nil {
			return nil, fmt.Errorf("KEKStore: mkdir %q: %w", keysDir, err)
		}
		if err := fsyncDir(parent); err != nil {
			return nil, fmt.Errorf("KEKStore: durability fsync after mkdir %q: %w", keysDir, err)
		}
	}

	entries, err := os.ReadDir(keysDir)
	if err != nil {
		return nil, fmt.Errorf("KEKStore: read %q: %w", keysDir, err)
	}

	s := NewKEKStore()
	loaded := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		v, ok := parseKeyFilename(e.Name())
		if !ok {
			continue // ignore other files (operator notes, etc.)
		}
		path := filepath.Join(keysDir, e.Name())
		kek, err := readKEKFile(path)
		if err != nil {
			return nil, err
		}
		if err := s.Add(v, kek); err != nil {
			return nil, fmt.Errorf("KEKStore: add v=%d from %s: %w", v, path, err)
		}
		loaded++
	}

	if loaded == 0 {
		// Fresh: generate v0
		kek := make([]byte, KEKSize)
		if _, err := io.ReadFull(rand.Reader, kek); err != nil {
			return nil, fmt.Errorf("KEKStore: generate v0: %w", err)
		}
		if err := writeKEKFileAtomic(filepath.Join(keysDir, "0.key"), kek); err != nil {
			return nil, fmt.Errorf("KEKStore: write v0: %w", err)
		}
		if err := s.Add(0, kek); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// AddAndPersist writes a KEK version to disk atomically THEN adds it to
// the in-memory store. Disk-first is critical: if Add succeeds but the
// disk write later fails, in-memory state would silently diverge from
// disk. Pre-flights with Lstat to refuse if the version is already on
// disk — this prevents the disk-first write from overwriting an existing
// KEK file with caller-supplied bytes, and prevents the rollback Remove
// from deleting a pre-existing file we should never have touched.
// On Add failure (e.g. duplicate in-memory but absent on disk), best-effort
// remove the file we just wrote.
func (s *KEKStore) AddAndPersist(keysDir string, version uint32, kek []byte) error {
	path := filepath.Join(keysDir, strconv.FormatUint(uint64(version), 10)+".key")
	if _, err := os.Lstat(path); err == nil {
		return fmt.Errorf("%w: %d (file %s already exists)", ErrKEKVersionDuplicate, version, path)
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("KEKStore.AddAndPersist: stat %q: %w", path, err)
	}
	if err := writeKEKFileAtomic(path, kek); err != nil {
		return err
	}
	if err := s.Add(version, kek); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

func readKEKFile(path string) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat %q: %w", path, err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		return nil, fmt.Errorf("KEK file %q has mode %#o, want 0o600", path, perm)
	}

	buf := make([]byte, KEKSize+1)
	n, err := io.ReadFull(f, buf[:KEKSize])
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", path, err)
	}
	if n != KEKSize {
		return nil, fmt.Errorf("KEK file %q has %d bytes, want %d", path, n, KEKSize)
	}
	extra, _ := f.Read(buf[KEKSize:])
	if extra > 0 {
		return nil, fmt.Errorf("KEK file %q exceeds %d bytes", path, KEKSize)
	}
	return buf[:KEKSize], nil
}

func writeKEKFileAtomic(path string, kek []byte) error {
	if len(kek) != KEKSize {
		return fmt.Errorf("writeKEKFileAtomic: kek len = %d, want %d", len(kek), KEKSize)
	}
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("open tmp %q: %w", tmp, err)
	}
	if _, err := f.Write(kek); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("write tmp %q: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return fmt.Errorf("fsync tmp %q: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("close tmp %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("rename %q -> %q: %w", tmp, path, err)
	}
	if err := fsyncDir(filepath.Dir(path)); err != nil {
		return fmt.Errorf("durability fsync after rename %q: %w", path, err)
	}
	return nil
}

// SealWithActiveKEK encrypts plain using the active KEK directly (NOT the
// wrapped-DEK keyring). Used by callers that need to sign blobs with cluster
// trust independently of DEK lifecycle, e.g. capability assertions.
// Returns nonce(12) + ciphertext + GCM-tag(16).
func (s *KEKStore) SealWithActiveKEK(plain, aad []byte) ([]byte, error) {
	kek, err := s.ActiveKEK()
	if err != nil {
		return nil, fmt.Errorf("SealWithActiveKEK: %w", err)
	}
	return aesgcmSealWithAAD(kek, plain, aad)
}

// OpenWithActiveKEK is the inverse of SealWithActiveKEK.
func (s *KEKStore) OpenWithActiveKEK(ct, aad []byte) ([]byte, error) {
	kek, err := s.ActiveKEK()
	if err != nil {
		return nil, fmt.Errorf("OpenWithActiveKEK: %w", err)
	}
	return aesgcmOpenWithAAD(kek, ct, aad)
}

// fsyncDir opens the directory at path and fsyncs it, ensuring that any
// preceding rename into that directory is durable across crashes. POSIX
// requires this for the rename to survive a power loss.
func fsyncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open dir %q for fsync: %w", path, err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("fsync dir %q: %w", path, err)
	}
	return nil
}

// WrapSetEntry is one entry in the canonical wrap-set hash input: a DEK
// generation number paired with its KEK-wrapped DEK bytes. Used by leaders
// to summarise the FSM's current wrap[] state as a deterministic 32-byte
// fingerprint for inclusion in MetaKEKRotateCmd.wrap_set_hash.
type WrapSetEntry struct {
	Gen  uint32
	Wrap []byte
}

// CanonicalWrapSetHash produces a deterministic 32-byte SHA-256 fingerprint
// over a wrap set. Entries are sorted ascending by gen, then each contributes
// uint32-big-endian(gen) || sha256(wrap) to the running hash. The inner hash
// keeps the input length-decoupled (two wraps of different size collide only
// on full sha256 collision), and the sort gives FSM-replay determinism.
func CanonicalWrapSetHash(entries []WrapSetEntry) [32]byte {
	sorted := make([]WrapSetEntry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Gen < sorted[j].Gen })
	h := sha256.New()
	for _, e := range sorted {
		var buf [4]byte
		binary.BigEndian.PutUint32(buf[:], e.Gen)
		h.Write(buf[:])
		inner := sha256.Sum256(e.Wrap)
		h.Write(inner[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}
