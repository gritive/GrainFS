package encrypt

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"syscall"
)

// KEKSize is the size of the Key Encryption Key in bytes (AES-256).
const KEKSize = 32

// ErrUnsupportedKEKSource is returned when the source scheme is not "file://".
var ErrUnsupportedKEKSource = errors.New("unsupported KEK source scheme (only file:// is supported in this release)")

// ErrKEKPermissionsTooLoose is returned when the KEK file exists with mode
// looser than 0o600. Operators must fix permissions, not GrainFS — silently
// loading a world-readable KEK would let any local user steal the cluster
// identity.
var ErrKEKPermissionsTooLoose = errors.New("KEK file permissions must be 0o600")

// ErrKEKSymlink is returned when the KEK path is a symlink. Refusing symlinks
// blocks an attacker with write access to the data directory from redirecting
// the KEK load to an attacker-chosen 32-byte file.
var ErrKEKSymlink = errors.New("KEK path must not be a symlink")

// ErrKEKNotFound is returned by LoadKEK when the KEK file is absent. The
// caller is expected to surface a remediation message (restore from backup /
// scp from a healthy peer / decommission and rejoin) — auto-generating a
// fresh KEK on a node that already holds FSM-wrapped DEKs would silently
// orphan all cluster state, which is F#21.
var ErrKEKNotFound = errors.New("KEK file not found")

// LoadOrGenerateKEK loads a KEK from source or generates a random one if the
// file does not exist. Only the "file://" scheme is supported; any other scheme
// returns ErrUnsupportedKEKSource.
//
// Auto-generation is appropriate ONLY for the very-first-node bootstrap path
// (a brand-new cluster with no prior FSM state). For any node that may have
// previously persisted FSM-wrapped DEKs (joiner, restart, snapshot Restore),
// use LoadKEK — which fails fast with ErrKEKNotFound rather than silently
// generating a wrong KEK that can never decrypt the cluster's DEKs.
//
// Security invariants enforced on every load:
//   - The path must be absolute. Relative paths or `..` traversal are rejected.
//   - The file MUST NOT be a symlink (O_NOFOLLOW) — guards against attacker
//     redirecting the KEK to a chosen file.
//   - The file mode MUST be exactly 0o600. Looser permissions are refused.
//
// If the file is missing it is created with permission 0o600 and filled with
// 32 cryptographically random bytes. If it exists its size must be exactly 32
// bytes; any other size is an error.
func LoadOrGenerateKEK(source string) ([]byte, error) {
	path, err := parseKEKSource(source)
	if err != nil {
		return nil, err
	}
	kek, err := readKEK(path)
	if err != nil {
		if errors.Is(err, ErrKEKNotFound) {
			return generateKEK(path)
		}
		return nil, err
	}
	return kek, nil
}

// LoadKEK loads a KEK from source. Unlike LoadOrGenerateKEK, it does NOT
// auto-generate when the file is absent — it returns ErrKEKNotFound so the
// caller can emit an operator-facing remediation path (F#21).
//
// Same security invariants as LoadOrGenerateKEK: file:// only, absolute path,
// no symlinks, exactly mode 0o600, exactly 32 bytes.
func LoadKEK(source string) ([]byte, error) {
	path, err := parseKEKSource(source)
	if err != nil {
		return nil, err
	}
	return readKEK(path)
}

// parseKEKSource validates the source URI and returns the absolute filesystem
// path. Only the "file://" scheme is supported.
func parseKEKSource(source string) (string, error) {
	const scheme = "file://"
	if !strings.HasPrefix(source, scheme) {
		return "", ErrUnsupportedKEKSource
	}
	path := source[len(scheme):]
	if !strings.HasPrefix(path, "/") {
		return "", fmt.Errorf("KEK path %q must be absolute", path)
	}
	return path, nil
}

// readKEK opens and validates a KEK file. Returns ErrKEKNotFound if the file
// does not exist; the caller decides whether to auto-generate (first-boot)
// or surface the missing-file error (any post-bootstrap path).
func readKEK(path string) ([]byte, error) {
	// O_NOFOLLOW: if the path is a symlink, open fails with ELOOP.
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("%w: %s", ErrKEKNotFound, path)
		}
		// ELOOP shows up as a syscall error wrapping ELOOP — translate it.
		var pe *os.PathError
		if errors.As(err, &pe) && errors.Is(pe.Err, syscall.ELOOP) {
			return nil, fmt.Errorf("%w: %s", ErrKEKSymlink, path)
		}
		return nil, fmt.Errorf("open KEK file %q: %w", path, err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat KEK file %q: %w", path, err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		return nil, fmt.Errorf("%w: %s has mode %#o", ErrKEKPermissionsTooLoose, path, perm)
	}

	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("read KEK file %q: %w", path, err)
	}
	if len(data) != KEKSize {
		return nil, fmt.Errorf("KEK file %q has %d bytes, expected %d", path, len(data), KEKSize)
	}
	return data, nil
}

func generateKEK(path string) ([]byte, error) {
	kek := make([]byte, KEKSize)
	if _, err := rand.Read(kek); err != nil {
		return nil, fmt.Errorf("generate KEK: %w", err)
	}
	// O_EXCL: refuse to overwrite a file that appeared between OpenFile and now.
	// O_NOFOLLOW: refuse to write through a symlink that an attacker just placed.
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create KEK file %q: %w", path, err)
	}
	if _, err := f.Write(kek); err != nil {
		f.Close()
		return nil, fmt.Errorf("write KEK file %q: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return nil, fmt.Errorf("close KEK file %q: %w", path, err)
	}
	return kek, nil
}
