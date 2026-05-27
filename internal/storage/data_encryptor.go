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
	aad := encrypt.BuildAAD(domain, a.clusterID, fields...)
	ct, err := a.enc.SealValueAADTo(nil, aad, plain)
	if err != nil {
		return nil, 0, err
	}
	return ct, 0, nil
}

func (a *EncryptorAdapter) Open(domain encrypt.AADDomain, fields []encrypt.AADField, _ uint32, ct []byte) ([]byte, error) {
	aad := encrypt.BuildAAD(domain, a.clusterID, fields...)
	return a.enc.OpenValueAADTo(nil, aad, ct)
}

var _ DataEncryptor = (*EncryptorAdapter)(nil)
