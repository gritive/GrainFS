package iam

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/storage"
)

// staticTestEncryptor returns a DEK-backed DataEncryptor seam at active gen 0.
// NewDEKKeeper randomizes the DEK, so the returned seam must be used for both
// wrap and unwrap within one test (every caller here does so).
func staticTestEncryptor(t testing.TB) storage.DataEncryptor {
	t.Helper()
	clusterID := []byte("0123456789abcdef")
	kek := make([]byte, encrypt.KEKSize)
	for i := range kek {
		kek[i] = byte(i + 1)
	}
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	return storage.NewDEKKeeperAdapter(keeper, clusterID)
}

// newTestEncryptor is a back-compat alias for the many test sites that still
// reference it.
func newTestEncryptor(t testing.TB) storage.DataEncryptor {
	return staticTestEncryptor(t)
}

func TestWrapUnwrapSecret_RoundTrip(t *testing.T) {
	de := staticTestEncryptor(t)
	ct, gen, err := WrapSecret(de, "sa-1", "AKIA-A", "secret123")
	require.NoError(t, err)
	require.NotEmpty(t, ct)
	require.Equal(t, uint32(0), gen, "DEK adapter seals at active gen 0")

	pt, err := UnwrapSecret(de, "sa-1", "AKIA-A", gen, ct)
	require.NoError(t, err)
	require.Equal(t, "secret123", pt)
}

func TestWrapUnwrapSecret_WrongSAIDRejects(t *testing.T) {
	de := staticTestEncryptor(t)
	ct, gen, err := WrapSecret(de, "sa-1", "AKIA-A", "secret123")
	require.NoError(t, err)
	_, err = UnwrapSecret(de, "sa-OTHER", "AKIA-A", gen, ct)
	require.Error(t, err, "AAD bind to sa_id must reject cross-SA replay")
}

// Codex P1 regression: two access keys under the SAME SA must not have
// swappable ciphertexts.
func TestWrapUnwrapSecret_CrossKeyReplayRejected(t *testing.T) {
	de := staticTestEncryptor(t)
	ctA, gen, err := WrapSecret(de, "sa-1", "AKIA-A", "secret-A")
	require.NoError(t, err)
	_, err = UnwrapSecret(de, "sa-1", "AKIA-B", gen, ctA)
	require.Error(t, err, "AAD bind to access_key must reject cross-key replay within an SA")
}

// BucketUpstream uses saID="bucket-upstream:"+bucket, accessKey="" — the
// bucket itself is the discriminator; access_key field is empty by convention.
func TestWrapUnwrapSecret_BucketUpstreamRoundTrip(t *testing.T) {
	de := staticTestEncryptor(t)
	bucketSAID := "bucket-upstream:my-bucket"
	ct, gen, err := WrapSecret(de, bucketSAID, "", "upstream-secret")
	require.NoError(t, err)
	pt, err := UnwrapSecret(de, bucketSAID, "", gen, ct)
	require.NoError(t, err)
	require.Equal(t, "upstream-secret", pt)
}

func TestWrapSecret_NilEncryptorErrors(t *testing.T) {
	_, _, err := WrapSecret(nil, "sa-1", "AKIA-A", "x")
	require.Error(t, err)
}

func TestUnwrapSecret_NilEncryptorErrors(t *testing.T) {
	_, err := UnwrapSecret(nil, "sa-1", "AKIA-A", 0, []byte{0x01})
	require.Error(t, err)
}
