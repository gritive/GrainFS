package encrypt

import (
	"crypto/sha256"
	"fmt"

	"golang.org/x/crypto/argon2"
	"golang.org/x/crypto/hkdf"
)

// envKEKSaltSize is the per-install random salt length for both KDFs.
const envKEKSaltSize = 32

// hkdfInfo domain-separates the env-binding HKDF from any other HKDF use.
const hkdfInfo = "grainfs-kek-env-v1"

// Argon2id default cost parameters for the recovery-passphrase KDF. The actual
// values used to seal a container are stored in that container, so a later boot
// re-derives with the file's params even if these defaults are tuned upward.
const (
	argonTimeDefault    uint32 = 1
	argonMemDefault     uint32 = 64 * 1024 // 64 MiB, in KiB
	argonThreadsDefault uint8  = 4
)

// deriveEEK derives the environmental encryption key from canonical machine-factor
// IKM and the per-install salt via HKDF-SHA256. The IKM is high-entropy (machine
// identifiers), so HKDF — not a password hash — is the right primitive.
func deriveEEK(ikm, salt []byte) ([]byte, error) {
	r := hkdf.New(sha256.New, ikm, salt, []byte(hkdfInfo))
	key := make([]byte, KEKSize)
	if _, err := r.Read(key); err != nil {
		return nil, fmt.Errorf("deriveEEK: %w", err)
	}
	return key, nil
}

// deriveRecoveryKey derives the recovery-slot key from a low-entropy operator
// passphrase via Argon2id. Cost parameters are passed in (read from the
// container on unwrap; defaults at create) so param tuning never strands old
// files.
func deriveRecoveryKey(passphrase, salt []byte, time, mem uint32, threads uint8) []byte {
	return argon2.IDKey(passphrase, salt, time, mem, threads, KEKSize)
}
