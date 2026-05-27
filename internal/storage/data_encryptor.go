package storage

import "github.com/gritive/GrainFS/internal/encrypt"

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
	Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) (plain []byte, err error)
}

// buildSeamAAD is the single AAD-construction point shared by every adapter, so
// the seam's AAD shape can never drift between implementations.
func buildSeamAAD(clusterID []byte, domain encrypt.AADDomain, fields []encrypt.AADField) []byte {
	return encrypt.BuildAAD(domain, clusterID, fields...)
}

// EncryptorAdapter implements DataEncryptor over the static encrypt.Encryptor.
// It always seals at the sentinel generation 0 and ignores the gen argument on
// Open. clusterID MUST be 16 bytes (BuildAAD panics otherwise).
type EncryptorAdapter struct {
	enc       *encrypt.Encryptor
	clusterID []byte
}

// NewEncryptorAdapter wraps enc so it satisfies DataEncryptor. clusterID is
// bound into every AAD via encrypt.BuildAAD.
func NewEncryptorAdapter(enc *encrypt.Encryptor, clusterID []byte) *EncryptorAdapter {
	return &EncryptorAdapter{enc: enc, clusterID: clusterID}
}

func (a *EncryptorAdapter) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	aad := buildSeamAAD(a.clusterID, domain, fields)
	ct, err := a.enc.SealValueAADTo(nil, aad, plain)
	if err != nil {
		return nil, 0, err
	}
	return ct, 0, nil
}

func (a *EncryptorAdapter) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	aad := buildSeamAAD(a.clusterID, domain, fields)
	return a.enc.OpenValueAADTo(nil, aad, ct)
}

var _ DataEncryptor = (*EncryptorAdapter)(nil)

// DEKKeeperAdapter implements DataEncryptor over the generation-aware
// encrypt.DEKKeeper. Seal uses the active generation; Open uses the supplied
// generation. clusterID MUST be 16 bytes.
type DEKKeeperAdapter struct {
	keeper    *encrypt.DEKKeeper
	clusterID []byte
}

// NewDEKKeeperAdapter wraps keeper so it satisfies DataEncryptor.
func NewDEKKeeperAdapter(keeper *encrypt.DEKKeeper, clusterID []byte) *DEKKeeperAdapter {
	return &DEKKeeperAdapter{keeper: keeper, clusterID: clusterID}
}

func (a *DEKKeeperAdapter) Seal(domain encrypt.AADDomain, fields []encrypt.AADField, plain []byte) ([]byte, uint32, error) {
	aad := buildSeamAAD(a.clusterID, domain, fields)
	return a.keeper.SealWithAAD(plain, aad)
}

func (a *DEKKeeperAdapter) Open(domain encrypt.AADDomain, fields []encrypt.AADField, gen uint32, ct []byte) ([]byte, error) {
	aad := buildSeamAAD(a.clusterID, domain, fields)
	return a.keeper.OpenWithAAD(ct, gen, aad)
}

var _ DataEncryptor = (*DEKKeeperAdapter)(nil)
