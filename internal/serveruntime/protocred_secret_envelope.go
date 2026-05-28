package serveruntime

import (
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/protocred"
)

type protocolCredentialSecretEnvelope struct {
	enc *encrypt.Encryptor
}

func (e protocolCredentialSecretEnvelope) SealProtocolCredentialSecret(aad []byte, plaintext string) ([]byte, error) {
	return e.enc.EncryptWithAAD([]byte(plaintext), aad)
}

func (e protocolCredentialSecretEnvelope) OpenProtocolCredentialSecret(aad []byte, ciphertext []byte) (string, error) {
	plain, err := e.enc.DecryptWithAAD(ciphertext, aad)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func protocolCredentialEnvelopeFromState(state *bootState) protocred.SecretEnvelope {
	if state == nil || state.cfg.Encryptor == nil {
		return nil
	}
	return protocolCredentialSecretEnvelope{enc: state.cfg.Encryptor}
}
