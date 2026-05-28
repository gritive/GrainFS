package serveruntime

import (
	"encoding/binary"
	"fmt"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/storage"
)

var protocolCredentialSecretMagic = [4]byte{'G', 'P', 'C', 1}

type protocolCredentialSecretEnvelope struct {
	enc storage.DataEncryptor
}

func (e protocolCredentialSecretEnvelope) SealProtocolCredentialSecret(aad []byte, plaintext string) ([]byte, error) {
	ct, gen, err := e.enc.Seal(
		encrypt.DomainProtocolCredential,
		[]encrypt.AADField{encrypt.FieldBytes(aad)},
		[]byte(plaintext),
	)
	if err != nil {
		return nil, err
	}
	out := make([]byte, 8, 8+len(ct))
	copy(out[:4], protocolCredentialSecretMagic[:])
	binary.BigEndian.PutUint32(out[4:8], gen)
	out = append(out, ct...)
	return out, nil
}

func (e protocolCredentialSecretEnvelope) OpenProtocolCredentialSecret(aad []byte, ciphertext []byte) (string, error) {
	if len(ciphertext) < 8 || string(ciphertext[:4]) != string(protocolCredentialSecretMagic[:]) {
		return "", fmt.Errorf("protocol credential secret envelope: invalid frame")
	}
	gen := binary.BigEndian.Uint32(ciphertext[4:8])
	plain, err := e.enc.Open(
		encrypt.DomainProtocolCredential,
		[]encrypt.AADField{encrypt.FieldBytes(aad)},
		gen,
		ciphertext[8:],
	)
	if err != nil {
		return "", err
	}
	return string(plain), nil
}

func protocolCredentialEnvelopeFromState(state *bootState) protocred.SecretEnvelope {
	if state == nil || state.dekKeeper == nil || len(state.clusterID) != 16 {
		return nil
	}
	return protocolCredentialSecretEnvelope{enc: storage.NewDEKKeeperAdapter(state.dekKeeper, state.clusterID)}
}
