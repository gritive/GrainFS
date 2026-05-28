package iam

import (
	"errors"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// errEncryptorNil is returned when a nil DataEncryptor is passed to
// WrapSecret/UnwrapSecret. Caller bug — IAM store must always be
// constructed with a non-nil encryptor (or one wired in via SetEncryptor
// before the first apply).
var errEncryptorNil = errors.New("iam: encryptor is nil")

// credentialAADFields builds the AAD field list for a credential. saID is the
// owning ServiceAccount ID for SA credentials, or the conventional
// "bucket-upstream:"+bucket sentinel for BucketUpstream entries — the call
// site decides which. accessKey is the specific AccessKey.AccessKey for SA
// credentials (so two keys under the same SA cannot have their ciphertexts
// swapped without AEAD rejection); for BucketUpstream call sites pass "".
func credentialAADFields(saID, accessKey string) []encrypt.AADField {
	return []encrypt.AADField{
		encrypt.FieldString(saID),
		encrypt.FieldString(accessKey),
	}
}

// WrapSecret seals plaintext under the DEK via the DataEncryptor seam,
// AAD-bound to (saID, accessKey) under DomainIAMCredential. Returns
// ciphertext + DEK generation. Callers MUST persist both — UnwrapSecret needs
// the gen to look up the sealing key, and the same (saID, accessKey) tuple to
// satisfy AEAD.
func WrapSecret(enc storage.DataEncryptor, saID, accessKey, plaintext string) ([]byte, uint32, error) {
	if enc == nil {
		return nil, 0, errEncryptorNil
	}
	return enc.Seal(encrypt.DomainIAMCredential, credentialAADFields(saID, accessKey), []byte(plaintext))
}

// UnwrapSecret reverses WrapSecret. The gen MUST match what WrapSecret
// returned; a mismatch (wrong DEK generation, wrong accessKey, or wrong
// saID) returns a non-nil error.
func UnwrapSecret(enc storage.DataEncryptor, saID, accessKey string, gen uint32, ct []byte) (string, error) {
	if enc == nil {
		return "", errEncryptorNil
	}
	pt, err := enc.Open(encrypt.DomainIAMCredential, credentialAADFields(saID, accessKey), gen, ct)
	if err != nil {
		return "", err
	}
	return string(pt), nil
}
