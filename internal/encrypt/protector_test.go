package encrypt

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPlaintextProtector_Identity(t *testing.T) {
	p := PlaintextProtector{}
	require.Equal(t, "plaintext", p.Name())

	kek := bytes.Repeat([]byte{0xAB}, KEKSize)
	blob, err := p.Protect(kek, []byte("aad"))
	require.NoError(t, err)
	// Identity: the on-disk blob must be byte-identical to the raw KEK so
	// existing <V>.key files stay exactly 32 bytes.
	require.True(t, bytes.Equal(kek, blob), "plaintext Protect must be identity")

	got, rewrap, err := p.Unprotect(blob, []byte("aad"))
	require.NoError(t, err)
	require.False(t, rewrap, "plaintext never asks for rewrap")
	require.True(t, bytes.Equal(kek, got))
}

func TestPlaintextProtector_ReturnsCopies(t *testing.T) {
	p := PlaintextProtector{}
	kek := bytes.Repeat([]byte{0x01}, KEKSize)

	blob, err := p.Protect(kek, nil)
	require.NoError(t, err)
	blob[0] ^= 0xFF // mutate the returned blob
	require.Equal(t, byte(0x01), kek[0], "Protect must return a copy, not alias input")

	got, _, err := p.Unprotect(kek, nil)
	require.NoError(t, err)
	got[0] ^= 0xFF
	require.Equal(t, byte(0x01), kek[0], "Unprotect must return a copy, not alias input")
}
