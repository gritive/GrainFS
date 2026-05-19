package encrypt

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"strings"
)

// KEKSize is the size of the Key Encryption Key in bytes (AES-256).
const KEKSize = 32

// ErrUnsupportedKEKSource is returned when the source scheme is not "file://".
var ErrUnsupportedKEKSource = errors.New("unsupported KEK source scheme (only file:// is supported in this release)")

// LoadOrGenerateKEK loads a KEK from source or generates a random one if the
// file does not exist. Only the "file://" scheme is supported; any other scheme
// returns ErrUnsupportedKEKSource.
//
// If the file is missing it is created with permission 0o600 and filled with
// 32 cryptographically random bytes. If it exists its size must be exactly 32
// bytes; any other size is an error.
func LoadOrGenerateKEK(source string) ([]byte, error) {
	const scheme = "file://"
	if !strings.HasPrefix(source, scheme) {
		return nil, ErrUnsupportedKEKSource
	}

	path := source[len(scheme):]

	data, err := os.ReadFile(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("read KEK file %q: %w", path, err)
		}
		// File does not exist — generate a new random KEK.
		kek := make([]byte, KEKSize)
		if _, err := rand.Read(kek); err != nil {
			return nil, fmt.Errorf("generate KEK: %w", err)
		}
		if err := os.WriteFile(path, kek, 0o600); err != nil {
			return nil, fmt.Errorf("write KEK file %q: %w", path, err)
		}
		return kek, nil
	}

	if len(data) != KEKSize {
		return nil, fmt.Errorf("KEK file %q has %d bytes, expected %d", path, len(data), KEKSize)
	}
	return data, nil
}
