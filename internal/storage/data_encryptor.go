package storage

import (
	"sync"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// DataEncryptor is the data-at-rest encryption seam. It hides whether the
// underlying key material is the static Encryptor (legacy) or the
// generation-aware DEKKeeper (KEK-envelope target). Callers supply the AAD
// domain + fields; the seam builds the canonical AAD via encrypt.BuildAAD.
//
// Seal returns the DEK generation the ciphertext was sealed under (0 for the
// static Encryptor, which has no generations). Open takes that generation back
// so a generation-aware implementation can select the right key; the Encryptor
// implementation ignores it.
type DataEncryptor interface {
	Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) (ct []byte, gen uint32, err error)
	// SealTo is Seal that appends the ciphertext into dst, reusing dst's
	// capacity when it suffices. The output is byte-equivalent to Seal.
	SealTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) (ct []byte, gen uint32, err error)
	Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
	// OpenTo is Open that appends the plaintext into dst, reusing dst's
	// capacity when it suffices. The output is byte-equivalent to Open.
	// dst and ct MUST NOT overlap (cipher.AEAD.Open contract).
	OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
}

// seamAADPool recycles the scratch buffer used to build the canonical AAD for
// SealTo. The AAD is only consumed as GCM associated data (never retained), so
// reusing the backing array across calls is safe.
var seamAADPool = sync.Pool{New: func() any { b := make([]byte, 0, 128); return &b }}

// withSeamAAD builds the canonical AAD into a pooled scratch buffer, invokes fn
// with it, then returns the buffer to the pool.
func withSeamAAD(clusterID []byte, domain encrypt.AADDomain, fields []encrypt.AADField, fn func(aad []byte) ([]byte, uint32, error)) ([]byte, uint32, error) {
	p := seamAADPool.Get().(*[]byte)
	aad := encrypt.AppendAAD((*p)[:0], domain, clusterID, fields...)
	ct, gen, err := fn(aad)
	*p = aad[:0]
	seamAADPool.Put(p)
	return ct, gen, err
}

// withSeamAADErr2 is withSeamAAD for the Open path: same pooled AAD scratch,
// but fn returns ([]byte, error) (Open has no generation to report). The AAD is
// only consumed as GCM associated data (never retained), so reuse is safe.
func withSeamAADErr2(clusterID []byte, domain encrypt.AADDomain, fields []encrypt.AADField, fn func(aad []byte) ([]byte, error)) ([]byte, error) {
	p := seamAADPool.Get().(*[]byte)
	aad := encrypt.AppendAAD((*p)[:0], domain, clusterID, fields...)
	plain, err := fn(aad)
	*p = aad[:0]
	seamAADPool.Put(p)
	return plain, err
}

// buildSeamAAD is the AAD-construction point shared by every adapter's Seal.
// Both it and withSeamAAD (the pooled SealTo path) funnel through
// encrypt.AppendAAD, so the seam's AAD shape can never drift between
// implementations or between Seal and SealTo.
func buildSeamAAD(clusterID []byte, domain encrypt.AADDomain, fields []encrypt.AADField) []byte {
	return encrypt.BuildAAD(domain, clusterID, fields...)
}

// DEKKeeperAdapter implements DataEncryptor over the generation-aware
// encrypt.DEKKeeper. Seal uses the active generation; Open uses the supplied
// generation. clusterID MUST be 16 bytes.
type DEKKeeperAdapter struct {
	keeper    *encrypt.DEKKeeper
	clusterID []byte
}

// NewDEKKeeperAdapter wraps keeper so it satisfies DataEncryptor.
func NewDEKKeeperAdapter(keeper *encrypt.DEKKeeper, clusterID []byte) *DEKKeeperAdapter {
	return &DEKKeeperAdapter{keeper: keeper, clusterID: append([]byte(nil), clusterID...)}
}

func (a *DEKKeeperAdapter) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	aad := buildSeamAAD(a.clusterID, domain, fields)
	return a.keeper.SealWithAAD(plain, aad)
}

func (a *DEKKeeperAdapter) SealTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	return withSeamAAD(a.clusterID, domain, fields, func(aad []byte) ([]byte, uint32, error) {
		return a.keeper.SealWithAADTo(dst, plain, aad)
	})
}

func (a *DEKKeeperAdapter) Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	aad := buildSeamAAD(a.clusterID, domain, fields)
	return a.keeper.OpenWithAAD(ct, gen, aad)
}

func (a *DEKKeeperAdapter) OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	return withSeamAADErr2(a.clusterID, domain, fields, func(aad []byte) ([]byte, error) {
		return a.keeper.OpenWithAADTo(dst, ct, gen, aad)
	})
}

var _ DataEncryptor = (*DEKKeeperAdapter)(nil)

// TransientDataEncryptor wraps an encrypt.TransientReadOnlyDEK to satisfy
// the DataEncryptor seam during MetaFSM.Restore. It is used to decrypt
// DEK-sealed trailers (e.g. IAM credentials) BEFORE the live DEKKeeper is
// wired by boot. Seal is unsupported — see encrypt.ErrTransientReadOnly:
// sealing through this view would write ciphertext under a stale
// generation map and silently break gen tracking.
//
// clusterID is bound into the AAD via encrypt.BuildAAD, matching
// DEKKeeperAdapter so a transient adapter and a live adapter Open the same
// ciphertext.
type TransientDataEncryptor struct {
	inner     *encrypt.TransientReadOnlyDEK
	clusterID []byte
}

// NewTransientDataEncryptor wraps t. clusterID MUST be 16 bytes (BuildAAD
// panics otherwise).
func NewTransientDataEncryptor(t *encrypt.TransientReadOnlyDEK, clusterID []byte) *TransientDataEncryptor {
	return &TransientDataEncryptor{inner: t, clusterID: append([]byte(nil), clusterID...)}
}

func (a *TransientDataEncryptor) Seal(_ encrypt.AADDomain, _ []encrypt.AADField, _ []byte) ([]byte, uint32, error) {
	return nil, 0, encrypt.ErrTransientReadOnly
}

func (a *TransientDataEncryptor) SealTo(_ []byte, _ encrypt.AADDomain, _ []encrypt.AADField, _ []byte) ([]byte, uint32, error) {
	return nil, 0, encrypt.ErrTransientReadOnly
}

func (a *TransientDataEncryptor) Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	aad := buildSeamAAD(a.clusterID, domain, fields)
	return a.inner.OpenWithAAD(ct, gen, aad)
}

func (a *TransientDataEncryptor) OpenTo(dst []byte, domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	return withSeamAADErr2(a.clusterID, domain, fields, func(aad []byte) ([]byte, error) {
		return a.inner.OpenWithAADTo(dst, ct, gen, aad)
	})
}

var _ DataEncryptor = (*TransientDataEncryptor)(nil)
