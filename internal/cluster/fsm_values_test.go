package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestFSMOpenValueRejectsOldFormatEncrypted(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)

	f := &FSM{}
	f.enc = enc

	key := []byte("cluster-fsm:test-key")
	// Old-format encrypted value: 0xAE 0xE2 (value magic) + version 0x01 (pre-XAES)
	oldFormatVal := []byte{0xAE, 0xE2, 0x01, 0xDE, 0xAD, 0xBE, 0xEF}

	_, err = f.openValue(key, oldFormatVal)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported/old encrypted-value format")
}

func TestFSMOpenValueRejectsOldFormatEncryptedWithoutEncryptor(t *testing.T) {
	// enc == nil branch: a legacy v1 value must loud-fail, not pass through as
	// raw plaintext.
	f := &FSM{}

	key := []byte("cluster-fsm:test-key")
	oldFormatVal := []byte{0xAE, 0xE2, 0x01, 0xDE, 0xAD, 0xBE, 0xEF}

	_, err := f.openValue(key, oldFormatVal)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported/old encrypted-value format")
}

func TestFSMOpenValuePassesGenuinePlaintextWithoutEncryptor(t *testing.T) {
	// enc == nil branch: genuine plaintext (no/!= legacy signature) still passes.
	f := &FSM{}

	key := []byte("cluster-fsm:test-key")
	// Value magic prefix but version 0x05 (neither legacy 0x01 nor current 0x02).
	plain := []byte{0xAE, 0xE2, 0x05, 'd', 'a', 't', 'a'}

	got, err := f.openValue(key, plain)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

func TestFSMOpenValuePassesGenuinePlaintext(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)

	f := &FSM{}
	f.enc = enc

	key := []byte("cluster-fsm:test-key")
	// Genuine plaintext: no magic bytes at all
	plain := []byte(`{"key":"value"}`)

	got, err := f.openValue(key, plain)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

func TestFSMOpenValueRoundTrip(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)

	f := &FSM{}
	f.enc = enc

	key := []byte("cluster-fsm:test-key")
	plain := []byte("mutation body")

	sealed, err := f.sealValue(key, plain)
	require.NoError(t, err)

	got, err := f.openValue(key, sealed)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}
