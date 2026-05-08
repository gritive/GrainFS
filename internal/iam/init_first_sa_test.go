package iam

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
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

// TestApplyInitFirstSA_PartialFail_RetryFillsMissingSteps asserts that if
// ApplyInitFirstSA committed only the SA (no key, no grant) on a prior
// run — simulated by directly calling applySACreate, which mimics the
// state after ApplyKeyCreate would have errored — a subsequent
// ApplyInitFirstSA fires the missing key + wildcard grant rather than
// taking the idempotent-skip branch. Regresses against an SA-presence-
// only check that would leave the cluster permanently half-bootstrapped.
func TestApplyInitFirstSA_PartialFail_RetryFillsMissingSteps(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	store := NewStore()
	applier := NewApplier(store, enc)

	// Simulate a prior partial bootstrap: SA committed, key + grant absent
	// (e.g. ApplyKeyCreate hit a transient encrypt error after the SA
	// proposal already replicated). This is the failure mode the strong
	// idempotency predicate must recover from.
	now := time.Now().UTC()
	store.applySACreate(ServiceAccount{ID: DefaultSAID, Name: "admin", CreatedAt: now})
	require.False(t, isFirstSACommitted(store), "partial state must NOT be considered complete")

	// Retry: full ApplyInitFirstSA must fill in the missing steps.
	payload := buildTestInitFirstSAPayload(t, enc, "AKIA-retry", "sec-retry")
	require.NoError(t, applier.ApplyInitFirstSA(payload))

	require.True(t, isFirstSACommitted(store), "after retry, all 3 records must be present")
	_, ok := store.LookupKey("AKIA-retry")
	require.True(t, ok, "retry must commit the key that the partial run missed")
	require.Equal(t, RoleAdmin, store.LookupGrant(DefaultSAID, "any-bucket"),
		"retry must commit the wildcard grant that the partial run missed")
}

func TestSnapshotRoundtripV3(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)

	src := NewStore()
	applier := NewApplier(src, enc)

	payload := buildTestInitFirstSAPayload(t, enc, "AKIA-snap", "sec-snap")
	require.NoError(t, applier.ApplyInitFirstSA(payload))

	var buf bytes.Buffer
	require.NoError(t, WriteSnapshot(&buf, src))

	// Header byte must be version 3.
	require.Equal(t, uint8(3), buf.Bytes()[0], "snapshot header must be v3")
	// V2 had an authBit byte at offset 1; v3 must NOT.
	// Next byte at offset 1 must be the start of the SA count u32 (LE).
	// We don't check exact value, just that decoding succeeds.

	dst := NewStore()
	require.NoError(t, ReadSnapshot(&buf, dst, enc))

	sa, ok := dst.LookupSA(DefaultSAID)
	require.True(t, ok)
	require.Equal(t, "admin", sa.Name)

	role := dst.LookupGrant(DefaultSAID, "anybucket")
	require.Equal(t, RoleAdmin, role)
}

func TestReadSnapshot_RejectsV1V2(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)

	for _, ver := range []byte{1, 2} {
		buf := bytes.NewReader([]byte{ver, 0, 0, 0, 0, 0}) // dummy header
		dst := NewStore()
		err := ReadSnapshot(buf, dst, enc)
		require.Error(t, err, "v%d must be rejected", ver)
		require.Contains(t, err.Error(), "snapshot version")
	}
}

// TestHandleSACreate_EmptyStore_DispatchesInitFirstSA verifies that on an
// empty store the handler routes through ProposeInitFirstSA (composite cmd)
// rather than the legacy 3-call SACreate+KeyCreate+GrantWildcardPut path,
// and that the response advertises the auto-issued wildcard grant.
func TestHandleSACreate_EmptyStore_DispatchesInitFirstSA(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	store := NewStore()
	prop := &fakeProposer{store: store, enc: enc}
	api := NewAdminAPI(store, prop, enc)

	req := httptest.NewRequest("POST", "/v1/iam/sa",
		strings.NewReader(`{"name":"admin","description":"first admin"}`))
	rec := httptest.NewRecorder()

	api.HandleSACreate(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, []string{"InitFirstSA"}, prop.dispatched, "must use composite cmd, not 3 individuals")

	// Verify response shape: must include grants:[{bucket:"*", role:"admin"}].
	require.Contains(t, rec.Body.String(), `"grants"`)
	require.Contains(t, rec.Body.String(), `"bucket":"*"`)
	require.Contains(t, rec.Body.String(), `"role":"admin"`)
}

// TestHandleSACreate_NonEmptyStore_DispatchesSACreatePlusKey verifies that
// the second-and-onward SA goes through the regular two-step propose path
// (SACreate → KeyCreate) and is NOT auto-granted any bucket access.
func TestHandleSACreate_NonEmptyStore_DispatchesSACreatePlusKey(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	store := NewStore()
	// Pre-seed: an existing SA so the store is non-empty.
	store.applySACreate(ServiceAccount{ID: DefaultSAID, Name: "admin"})

	prop := &fakeProposer{store: store, enc: enc}
	api := NewAdminAPI(store, prop, enc)

	req := httptest.NewRequest("POST", "/v1/iam/sa",
		strings.NewReader(`{"name":"user1"}`))
	rec := httptest.NewRecorder()

	api.HandleSACreate(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, "body=%s", rec.Body.String())
	require.Equal(t, []string{"SACreate", "KeyCreate"}, prop.dispatched,
		"must use individual cmds + no wildcard grant")
	require.NotContains(t, rec.Body.String(), `"grants"`,
		"non-bootstrap path must not emit a grants field")
}

// TestHandleSACreate_RaceConflict_Returns409 simulates the case where a
// concurrent operator wins the bootstrap race between IsEmpty() and
// ProposeInitFirstSA. The store starts empty (so the handler enters the
// InitFirstSA branch), but the fake proposer — with initBlocked=true and
// a raceWinnerKey configured — mimics the FSM dropping our payload and
// committing the winner's records instead. Post-propose LookupKey for
// our access_key fails → 409.
func TestHandleSACreate_RaceConflict_Returns409(t *testing.T) {
	enc, err := encrypt.NewEncryptor(make([]byte, 32))
	require.NoError(t, err)
	store := NewStore()

	prop := &fakeProposer{
		store:       store,
		enc:         enc,
		initBlocked: true,
		raceWinnerKey: AccessKey{
			AccessKey: "AKIA-winner", SAID: DefaultSAID, Status: KeyStatusActive,
		},
	}
	api := NewAdminAPI(store, prop, enc)

	req := httptest.NewRequest("POST", "/v1/iam/sa",
		strings.NewReader(`{"name":"loser"}`))
	rec := httptest.NewRecorder()

	api.HandleSACreate(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code, "race detection must return 409, body=%s", rec.Body.String())
	require.Equal(t, []string{"InitFirstSA"}, prop.dispatched)
}
