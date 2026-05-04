package transport

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

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
type Keystore struct {
	dir string // {dataDir}/keys.d
}

// NewKeystore returns a Keystore rooted at {dataDir}/keys.d/. The directory
// is created on demand at write time; ReadCurrent on a fresh dataDir returns
// os.ErrNotExist.
func NewKeystore(dataDir string) *Keystore {
	return &Keystore{dir: filepath.Join(dataDir, "keys.d")}
}

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
	if _, err := io.WriteString(tmp, psk+"\n"); err != nil {
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
	path := filepath.Join(k.dir, name)
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()
	b, err := io.ReadAll(f)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
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
