package encrypt

// KeyProtector wraps and unwraps key material for at-rest storage. It is the
// seam between the KEK store (which persists <V>.key files) and the mechanism
// that protects those bytes on disk. Implementations: PlaintextProtector
// (identity, default), EnvProtector (machine-binding + recovery passphrase).
// Future KMS/HSM/TPM backends map onto the same Protect/Unprotect envelope.
type KeyProtector interface {
	// Protect wraps plaintext key material for at-rest storage. aad binds the
	// blob to its logical identity (e.g. the KEK version) and is reproduced
	// verbatim on Unprotect.
	Protect(plaintext, aad []byte) (blob []byte, err error)

	// Unprotect reverses Protect. rewrap=true signals the caller SHOULD
	// re-Protect and re-persist the returned plaintext (e.g. the env binding
	// was rebound to new machine factors, or a legacy raw file was migrated).
	// On unwrap failure it returns a non-nil error and the caller MUST fail
	// closed — never regenerate.
	Unprotect(blob, aad []byte) (plaintext []byte, rewrap bool, err error)

	// Name identifies the provider for logs/metrics ("plaintext", "env", ...).
	Name() string
}

// PlaintextProtector is the identity protector: at-rest bytes equal the raw key
// material. It is the default so existing <V>.key files remain byte-identical
// (exactly KEKSize bytes) and no migration is needed when the feature is off.
type PlaintextProtector struct{}

// Protect returns a copy of plaintext unchanged.
func (PlaintextProtector) Protect(plaintext, _ []byte) ([]byte, error) {
	out := make([]byte, len(plaintext))
	copy(out, plaintext)
	return out, nil
}

// Unprotect returns a copy of blob unchanged, never requesting a rewrap.
func (PlaintextProtector) Unprotect(blob, _ []byte) ([]byte, bool, error) {
	out := make([]byte, len(blob))
	copy(out, blob)
	return out, false, nil
}

// Name returns "plaintext".
func (PlaintextProtector) Name() string { return "plaintext" }
