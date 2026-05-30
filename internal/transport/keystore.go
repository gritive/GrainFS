package transport

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// pskAAD is the slot-INVARIANT AEAD associated-data for every cluster-key PSK
// slot. It deliberately contains NO slot name and NO version: MoveNextToCurrent
// promotes slots by renaming the sealed blob (next→current→previous), so a blob
// sealed for one slot must open after being renamed into another. Binding the
// slot name into the AAD (as Slice 1 bound the KEK version) would break rename
// promotion by construction. This is a deliberate inversion of the KEK design —
// here the three slots are one key's lifecycle, not independent keys.
const pskAAD = "grainfs-cluster-key-psk-v1"

// Keystore manages the persistent on-disk cluster identity keys at
// {dataDir}/keys.d/. Three slots:
//
//   - current.key   active identity (always present after first start)
//   - next.key      phase 2/3 candidate (operator-distributed out-of-band)
//   - previous.key  post-rotation grace (deleted at next phase-1 entry OR +24h)
//
// All file I/O is hardened: directory 0700, files 0600, atomic temp+rename
// with fsync of file and parent dir, O_NOFOLLOW on every open to defeat
// symlink traversal if an attacker has write access to keys.d/.
//
// At-rest protection: each slot's bytes pass through a KeyProtector (Slice 2).
// PlaintextProtector (default) keeps the on-disk format byte-identical to the
// historical `psk+"\n"`. EnvProtector wraps the PSK in a machine-bound container
// (same seam + --kek-protector gate as the KEK store). readSlot only UNWRAPS;
// rewrap-on-rebind is done by boot callers (see serveruntime), never inside the
// primitive, because Keystore is mutated concurrently by the rotation worker and
// the previous-key cleanup timer with no mutex.
type Keystore struct {
	dir       string // {dataDir}/keys.d
	protector encrypt.KeyProtector
}

// NewKeystore returns a Keystore rooted at {dataDir}/keys.d/ with the identity
// (plaintext) protector — on-disk bytes stay byte-identical to the historical
// format. The directory is created on demand at write time; ReadCurrent on a
// fresh dataDir returns os.ErrNotExist.
func NewKeystore(dataDir string) *Keystore {
	return NewKeystoreWithProtector(dataDir, encrypt.PlaintextProtector{})
}

// NewKeystoreWithProtector is NewKeystore with an explicit at-rest KeyProtector.
func NewKeystoreWithProtector(dataDir string, protector encrypt.KeyProtector) *Keystore {
	return &Keystore{
		dir:       filepath.Join(dataDir, "keys.d"),
		protector: protector,
	}
}

func (k *Keystore) isPlaintext() bool { return k.protector.Name() == "plaintext" }

func (k *Keystore) ReadCurrent() (string, error)  { return k.readSlot("current.key") }
func (k *Keystore) ReadNext() (string, error)     { return k.readSlot("next.key") }
func (k *Keystore) ReadPrevious() (string, error) { return k.readSlot("previous.key") }

func (k *Keystore) WriteCurrent(psk string) error  { return k.writeSlot("current.key", psk) }
func (k *Keystore) WriteNext(psk string) error     { return k.writeSlot("next.key", psk) }
func (k *Keystore) WritePrevious(psk string) error { return k.writeSlot("previous.key", psk) }

func (k *Keystore) DeleteNext() error     { return k.deleteSlot("next.key") }
func (k *Keystore) DeletePrevious() error { return k.deleteSlot("previous.key") }

// MoveNextToCurrent atomically promotes next.key → current.key, demoting
// the existing current.key → previous.key. Used at phase 4 commit.
func (k *Keystore) MoveNextToCurrent() error {
	cur := filepath.Join(k.dir, "current.key")
	next := filepath.Join(k.dir, "next.key")
	prev := filepath.Join(k.dir, "previous.key")

	if _, err := os.Lstat(cur); err == nil {
		// Remove any stale previous.key first to avoid rename-over-existing.
		_ = os.Remove(prev)
		if err := os.Rename(cur, prev); err != nil {
			return fmt.Errorf("rename current → previous: %w", err)
		}
	}
	if err := os.Rename(next, cur); err != nil {
		return fmt.Errorf("rename next → current: %w", err)
	}
	return k.fsyncDir()
}

func (k *Keystore) ensureDir() error {
	if err := os.MkdirAll(k.dir, 0o700); err != nil {
		return fmt.Errorf("mkdir keys.d: %w", err)
	}
	// MkdirAll respects existing perms; tighten if dir was created with
	// looser umask before this code took ownership.
	return os.Chmod(k.dir, 0o700)
}

func (k *Keystore) writeSlot(name, psk string) error {
	if err := k.ensureDir(); err != nil {
		return err
	}
	path := filepath.Join(k.dir, name)
	tmpPath := path + ".tmp"
	// Best-effort cleanup of any prior crashed temp file.
	_ = os.Remove(tmpPath)
	tmp, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("create tmp: %w", err)
	}
	// Protect the at-rest bytes. Plaintext: the protector is identity, so we write
	// EXACTLY the historical `psk+"\n"` (byte-identical). Env: the container is
	// written RAW (no trailing newline — its bytes may end in any value).
	blob, err := k.protector.Protect([]byte(psk), []byte(pskAAD))
	if err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("protect %s: %w", name, err)
	}
	out := blob
	if k.isPlaintext() {
		out = append(blob, '\n')
	}
	if _, err := tmp.Write(out); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("write tmp: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("fsync tmp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("close tmp: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("rename tmp → %s: %w", name, err)
	}
	return k.fsyncDir()
}

func (k *Keystore) readSlot(name string) (string, error) {
	psk, _, err := k.readSlotRewrap(name)
	return psk, err
}

// readSlotRewrap reads and unwraps a slot, also reporting whether the protector
// signalled a rewrap (env binding changed, or legacy plaintext migrated). The
// public Read* methods and the runtime callers (rotation worker, cleanup timer)
// use readSlot and IGNORE rewrap — they MUST NOT rewrite, because Keystore has no
// mutex and those callers run concurrently. Only single-threaded BOOT callers
// (serveruntime cluster-key resolve) act on rewrap, via this method.
func (k *Keystore) readSlotRewrap(name string) (string, bool, error) {
	path := filepath.Join(k.dir, name)
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", false, err
	}
	defer f.Close()
	raw, err := io.ReadAll(f)
	if err != nil {
		return "", false, err
	}
	// Fail-loud on a wrong-protector misread: a plaintext-default keystore reading
	// an env container would otherwise return ~260 bytes of binary as a "PSK" with
	// no error (length-only validation can't catch it), yielding a wrong identity
	// that fails silently at the network handshake. The GKEK magic is the precise
	// discriminator and never appears in a legitimate plaintext `psk+"\n"`.
	if k.isPlaintext() && encrypt.LooksLikeEnvKEK(raw) {
		return "", false, fmt.Errorf("readSlot %s: env-protected container on disk but --kek-protector is plaintext (config mismatch)", name)
	}
	// Unprotect the RAW, untrimmed bytes — a container ends in a GCM tag that may
	// be a whitespace byte, so trimming before Unprotect would corrupt it (B1).
	pt, rewrap, err := k.protector.Unprotect(raw, []byte(pskAAD))
	if err != nil {
		return "", false, fmt.Errorf("readSlot %s: %w", name, err)
	}
	// TrimSpace only the recovered plaintext (strips the plaintext path's `\n`).
	return strings.TrimSpace(string(pt)), rewrap, nil
}

func (k *Keystore) deleteSlot(name string) error {
	path := filepath.Join(k.dir, name)
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return k.fsyncDir()
}

func (k *Keystore) fsyncDir() error {
	d, err := os.Open(k.dir)
	if err != nil {
		// Directory may not exist yet (e.g., delete-of-absent on fresh dir).
		// fsync of a non-existent dir is a no-op for our purposes.
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer d.Close()
	return d.Sync()
}
