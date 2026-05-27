package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestBootstrapSecretProviderReturnsAllSecrets(t *testing.T) {
	encKey := make([]byte, 32)
	for i := range encKey {
		encKey[i] = byte(0xA0 + i)
	}

	kek0 := make([]byte, encrypt.KEKSize)
	kek1 := make([]byte, encrypt.KEKSize)
	for i := range kek0 {
		kek0[i] = byte(i)
		kek1[i] = byte(0xFF - i)
	}

	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(0, kek0))
	require.NoError(t, store.Add(1, kek1))

	state := &bootState{
		cfg:          Config{RawEncryptionKey: encKey},
		transportPSK: "cluster-psk",
		kekStore:     store,
	}

	p := newBootstrapSecretProvider(state)
	gotEnc, gotGens, gotPSK, err := p.BootstrapSecrets()
	require.NoError(t, err)

	require.Equal(t, encKey, gotEnc)
	require.Equal(t, []byte("cluster-psk"), gotPSK)

	require.Len(t, gotGens, 2)
	require.Equal(t, uint32(0), gotGens[0].Gen)
	require.Equal(t, kek0, gotGens[0].Key)
	require.Equal(t, uint32(1), gotGens[1].Gen)
	require.Equal(t, kek1, gotGens[1].Key)
}

func TestBootstrapSecretProviderErrorsWithoutKEKStore(t *testing.T) {
	p := newBootstrapSecretProvider(&bootState{cfg: Config{}})
	_, _, _, err := p.BootstrapSecrets()
	require.Error(t, err)
}
