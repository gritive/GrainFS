package encrypt

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
)

// EnvProtector wraps key material under an environmental key (EEK) derived from
// machine-binding factors, with a second recovery slot keyed by an operator
// passphrase (LUKS-keyslot style — both slots wrap the SAME key). A binding
// change (hardware swap, NIC change, migration) is recoverable via the
// passphrase, after which the caller rebinds (re-Protect) to the new machine.
type EnvProtector struct {
	factors  FactorSource
	recovery func() ([]byte, error) // lazy recovery-secret resolver; (nil,nil)=absent
	saltGen  func() ([]byte, error) // 32 random bytes; injectable for tests
}

// NewEnvProtector builds an EnvProtector from the default host factor source and
// the given lazy recovery-secret resolver.
func NewEnvProtector(recovery func() ([]byte, error)) *EnvProtector {
	return &EnvProtector{
		factors:  DefaultFactorSource{},
		recovery: recovery,
		saltGen:  randomSalt,
	}
}

func randomSalt() ([]byte, error) {
	s := make([]byte, envKEKSaltSize)
	if _, err := rand.Read(s); err != nil {
		return nil, fmt.Errorf("env protector: salt: %w", err)
	}
	return s, nil
}

// Container framing (fixed binary; see plan):
//
//	magic   "GKEK" (4)
//	fmtVer  uint8  (1)   container FORMAT version (not the KEK version)
//	kekVer  uint32 (4)   debug/observability; the cross-version binding is the aad
//	argonT  uint32 (4)   Argon2id time
//	argonM  uint32 (4)   Argon2id memory (KiB)
//	argonP  uint8  (1)   Argon2id threads
//	salt    [32]
//	envLen  uint32 || env_slot   = AESGCMSealWithAAD(EEK,   KEK, slotAAD("env"))
//	recLen  uint32 || rec_slot   = AESGCMSealWithAAD(recKey, KEK, slotAAD("rec"))
const (
	envKEKMagicLen  = 4
	envKEKFmtVer    = 1
	envKEKHeaderLen = envKEKMagicLen + 1 + 4 + 4 + 4 + 1 + envKEKSaltSize // 50
)

var envKEKMagic = [4]byte{'G', 'K', 'E', 'K'}

// LooksLikeEnvKEK reports whether blob carries the env-protector container magic.
// A raw KEKSize-byte legacy file will not match.
func LooksLikeEnvKEK(blob []byte) bool {
	return len(blob) >= envKEKMagicLen &&
		blob[0] == envKEKMagic[0] && blob[1] == envKEKMagic[1] &&
		blob[2] == envKEKMagic[2] && blob[3] == envKEKMagic[3]
}

// Name returns "env".
func (p *EnvProtector) Name() string { return "env" }

// slotAAD binds a slot's ciphertext to the caller aad (carries the KEK version),
// the format version, the salt, and the slot identity — preventing slot-confusion
// and cross-version replay. Every variable-length field is length-prefixed
// (uint32 big-endian) so distinct (aad, salt, slotTag) tuples can never collide
// into the same AAD bytes regardless of what a future caller passes as aad.
func slotAAD(callerAAD, salt []byte, slotTag string) []byte {
	out := make([]byte, 0, 4+len(callerAAD)+1+4+len(salt)+4+len(slotTag))
	out = appendU32(out, uint32(len(callerAAD)))
	out = append(out, callerAAD...)
	out = append(out, envKEKFmtVer)
	out = appendU32(out, uint32(len(salt)))
	out = append(out, salt...)
	out = appendU32(out, uint32(len(slotTag)))
	out = append(out, slotTag...)
	return out
}

// Protect creates a fresh two-slot container wrapping plaintext. Requires the
// recovery secret (the recovery slot is mandatory).
func (p *EnvProtector) Protect(plaintext, aad []byte) ([]byte, error) {
	ikm, err := p.currentIKM()
	if err != nil {
		return nil, err
	}
	salt, err := p.saltGen()
	if err != nil {
		return nil, err
	}
	if len(salt) != envKEKSaltSize {
		return nil, fmt.Errorf("env protector: salt len %d, want %d", len(salt), envKEKSaltSize)
	}
	secret, err := p.requireRecovery()
	if err != nil {
		return nil, err
	}

	eek, err := deriveEEK(ikm, salt)
	if err != nil {
		return nil, err
	}
	recKey := deriveRecoveryKey(secret, salt, argonTimeDefault, argonMemDefault, argonThreadsDefault)
	// NOTE: secret is owned by the caller's resolver (it may return a shared or
	// reused backing array, e.g. a cached config value); do not zeroize it here.
	defer zeroize(eek)
	defer zeroize(recKey)

	envSlot, err := AESGCMSealWithAAD(eek, plaintext, slotAAD(aad, salt, "env"))
	if err != nil {
		return nil, fmt.Errorf("env protector: seal env slot: %w", err)
	}
	recSlot, err := AESGCMSealWithAAD(recKey, plaintext, slotAAD(aad, salt, "rec"))
	if err != nil {
		return nil, fmt.Errorf("env protector: seal recovery slot: %w", err)
	}
	return marshalEnvKEK(argonTimeDefault, argonMemDefault, argonThreadsDefault, salt, envSlot, recSlot), nil
}

// Unprotect opens a container (or migrates a legacy raw file). Returns rewrap=true
// when the caller should re-Protect and re-persist (binding changed, or legacy
// migration). Fails closed: an unopenable container is an error, never a regen.
func (p *EnvProtector) Unprotect(blob, aad []byte) ([]byte, bool, error) {
	if !LooksLikeEnvKEK(blob) {
		// Legacy raw KEK file: migrate into a container. Requires recovery.
		if len(blob) != KEKSize {
			return nil, false, fmt.Errorf("env protector: blob is neither a container nor a %d-byte raw KEK (len=%d)", KEKSize, len(blob))
		}
		if _, err := p.requireRecovery(); err != nil {
			return nil, false, fmt.Errorf("env protector: legacy migration needs a recovery secret: %w", err)
		}
		out := make([]byte, KEKSize)
		copy(out, blob)
		return out, true, nil
	}

	c, err := parseEnvKEK(blob)
	if err != nil {
		return nil, false, err
	}

	// Happy path: env slot opens with current factors. No recovery secret read.
	ikm, err := p.currentIKM()
	if err == nil {
		if eek, derr := deriveEEK(ikm, c.salt); derr == nil {
			pt, oerr := AESGCMOpenWithAAD(eek, c.envSlot, slotAAD(aad, c.salt, "env"))
			zeroize(eek)
			if oerr == nil {
				return pt, false, nil
			}
		}
	}

	// Recovery path: env slot failed (binding changed or factors unresolvable).
	secret, rerr := p.requireRecovery()
	if rerr != nil {
		return nil, false, fmt.Errorf("env protector: env binding unusable and no recovery secret: %w", rerr)
	}
	recKey := deriveRecoveryKey(secret, c.salt, c.argonT, c.argonM, c.argonP)
	// secret is caller-owned (see Protect) — do not zeroize.
	pt, oerr := AESGCMOpenWithAAD(recKey, c.recSlot, slotAAD(aad, c.salt, "rec"))
	zeroize(recKey)
	if oerr != nil {
		return nil, false, fmt.Errorf("env protector: recovery slot open failed (fail-closed): %w", oerr)
	}
	// Recovered — signal a rebind to the current machine factors.
	return pt, true, nil
}

func (p *EnvProtector) currentIKM() ([]byte, error) {
	factors, err := p.factors.Factors()
	if err != nil {
		return nil, fmt.Errorf("env protector: read factors: %w", err)
	}
	return CanonicalIKM(factors)
}

var errRecoveryAbsent = errors.New("recovery secret not configured")

func (p *EnvProtector) requireRecovery() ([]byte, error) {
	if p.recovery == nil {
		return nil, errRecoveryAbsent
	}
	secret, err := p.recovery()
	if err != nil {
		return nil, fmt.Errorf("resolve recovery secret: %w", err)
	}
	if len(secret) == 0 {
		return nil, errRecoveryAbsent
	}
	return secret, nil
}

type envKEKContainer struct {
	argonT, argonM   uint32
	argonP           uint8
	salt             []byte
	envSlot, recSlot []byte
}

func marshalEnvKEK(argonT, argonM uint32, argonP uint8, salt, envSlot, recSlot []byte) []byte {
	out := make([]byte, 0, envKEKHeaderLen+4+len(envSlot)+4+len(recSlot))
	out = append(out, envKEKMagic[:]...)
	out = append(out, envKEKFmtVer)
	out = appendU32(out, 0) // kekVer: debug field; the aad carries the authoritative version
	out = appendU32(out, argonT)
	out = appendU32(out, argonM)
	out = append(out, argonP)
	out = append(out, salt...)
	out = appendU32(out, uint32(len(envSlot)))
	out = append(out, envSlot...)
	out = appendU32(out, uint32(len(recSlot)))
	out = append(out, recSlot...)
	return out
}

func parseEnvKEK(blob []byte) (*envKEKContainer, error) {
	if len(blob) < envKEKHeaderLen {
		return nil, fmt.Errorf("env protector: blob too short (%d < %d)", len(blob), envKEKHeaderLen)
	}
	if !LooksLikeEnvKEK(blob) {
		return nil, fmt.Errorf("env protector: bad magic")
	}
	off := envKEKMagicLen
	if blob[off] != envKEKFmtVer {
		return nil, fmt.Errorf("env protector: unsupported format version %d", blob[off])
	}
	off++
	off += 4 // kekVer (debug only)
	c := &envKEKContainer{}
	c.argonT = binary.BigEndian.Uint32(blob[off:])
	off += 4
	c.argonM = binary.BigEndian.Uint32(blob[off:])
	off += 4
	c.argonP = blob[off]
	off++
	c.salt = blob[off : off+envKEKSaltSize]
	off += envKEKSaltSize

	envSlot, off, err := readLenPrefixed(blob, off, "env")
	if err != nil {
		return nil, err
	}
	recSlot, _, err := readLenPrefixed(blob, off, "recovery")
	if err != nil {
		return nil, err
	}
	c.envSlot, c.recSlot = envSlot, recSlot
	return c, nil
}

func readLenPrefixed(blob []byte, off int, name string) ([]byte, int, error) {
	if off+4 > len(blob) {
		return nil, 0, fmt.Errorf("env protector: truncated %s length", name)
	}
	n := int(binary.BigEndian.Uint32(blob[off:]))
	off += 4
	if n < 0 || off+n > len(blob) {
		return nil, 0, fmt.Errorf("env protector: truncated %s slot", name)
	}
	return blob[off : off+n], off + n, nil
}

func appendU32(b []byte, v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return append(b, buf[:]...)
}
