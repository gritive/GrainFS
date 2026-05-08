package iam

import (
	"errors"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// errEncryptorNil is returned when a nil *encrypt.Encryptor is passed to
// WrapSecret/UnwrapSecret. Caller bug — IAM store must always be
// constructed with a non-nil encryptor.
var errEncryptorNil = errors.New("iam: encryptor is nil")

// WrapSecret encrypts a plaintext secret_key with AAD = sa_id, binding the
// ciphertext to the owning ServiceAccount. Replay across SAs fails AAD
// check at Decrypt time.
func WrapSecret(enc *encrypt.Encryptor, saID, plaintext string) ([]byte, error) {
	if enc == nil {
		return nil, errEncryptorNil
	}
	return enc.EncryptWithAAD([]byte(plaintext), []byte(saID))
}

// UnwrapSecret decrypts a secret_key ciphertext with AAD = sa_id.
// Returns the plaintext for in-memory use. Wrong AAD or tampered
// ciphertext returns a non-nil error.
func UnwrapSecret(enc *encrypt.Encryptor, saID string, ct []byte) (string, error) {
	if enc == nil {
		return "", errEncryptorNil
	}
	pt, err := enc.DecryptWithAAD(ct, []byte(saID))
	if err != nil {
		return "", err
	}
	return string(pt), nil
}
