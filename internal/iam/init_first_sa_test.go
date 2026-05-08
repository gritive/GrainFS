package iam

import (
	"testing"
	"time"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam/iampb"
	"github.com/stretchr/testify/require"
)

// buildTestInitFirstSAPayload assembles a composite InitFirstSAPayload
// blob from concrete records, mirroring what the production proposer
// will emit once Task 3 wires it up.
func buildTestInitFirstSAPayload(t *testing.T, enc *encrypt.Encryptor, ak, sk string) []byte {
	t.Helper()
	now := time.Now().UTC()
	sa := ServiceAccount{ID: DefaultSAID, Name: "admin", CreatedAt: now}
	saBlob := buildSACreatePayload(sa)

	wrapped, err := WrapSecret(enc, DefaultSAID, sk)
	require.NoError(t, err)
	k := AccessKey{
		AccessKey:    ak,
		SecretKey:    sk,
		SecretKeyEnc: wrapped,
		SAID:         DefaultSAID,
		Status:       KeyStatusActive,
		CreatedAt:    now,
	}
	keyBlob := buildKeyCreatePayload(k)

	g := Grant{SAID: DefaultSAID, Bucket: WildcardBucket, Role: RoleAdmin, CreatedAt: now}
	gwBlob := buildGrantWildcardPutPayload(g)

	b := flatbuffers.NewBuilder(256)
	saOff := b.CreateByteVector(saBlob)
	kOff := b.CreateByteVector(keyBlob)
	gOff := b.CreateByteVector(gwBlob)
	iampb.InitFirstSAPayloadStart(b)
	iampb.InitFirstSAPayloadAddSaCreateBlob(b, saOff)
	iampb.InitFirstSAPayloadAddKeyCreateBlob(b, kOff)
	iampb.InitFirstSAPayloadAddGrantWildcardBlob(b, gOff)
	b.Finish(iampb.InitFirstSAPayloadEnd(b))
	return b.FinishedBytes()
}

func TestApplyInitFirstSA_EmptyStore_CreatesAllThree(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	store := NewStore()
	applier := NewApplier(store, enc)

	payload := buildTestInitFirstSAPayload(t, enc, "AKIA-test-001", "secret-test-001")
	require.NoError(t, applier.ApplyInitFirstSA(payload))

	sa, ok := store.LookupSA(DefaultSAID)
	require.True(t, ok)
	require.Equal(t, "admin", sa.Name)

	k, ok := store.LookupKey("AKIA-test-001")
	require.True(t, ok)
	require.Equal(t, DefaultSAID, k.SAID)
	require.Equal(t, "secret-test-001", k.SecretKey)

	role := store.LookupGrant(DefaultSAID, "any-bucket")
	require.Equal(t, RoleAdmin, role)
}

func TestApplyInitFirstSA_SecondApply_IdempotentSkip(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	store := NewStore()
	applier := NewApplier(store, enc)

	first := buildTestInitFirstSAPayload(t, enc, "AKIA-001", "sec-001")
	require.NoError(t, applier.ApplyInitFirstSA(first))

	second := buildTestInitFirstSAPayload(t, enc, "AKIA-002", "sec-002")
	require.NoError(t, applier.ApplyInitFirstSA(second))

	// First key must remain; second NOT inserted.
	_, ok := store.LookupKey("AKIA-001")
	require.True(t, ok, "first key must persist")
	_, ok = store.LookupKey("AKIA-002")
	require.False(t, ok, "second propose must be idempotent skip")
}
