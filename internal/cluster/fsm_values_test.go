package cluster

import (
	"bytes"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgermeta"
	"github.com/gritive/GrainFS/internal/badgerutil"
	"github.com/gritive/GrainFS/internal/encrypt"
)

func TestFSMOpenValueRejectsOldFormatEncrypted(t *testing.T) {
	// A legacy v1 (pre-XAES) value must loud-fail rather than pass through as
	// raw plaintext, regardless of which seam (if any) is wired.
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
	// No seam wired: genuine plaintext (no magic bytes) passes through.
	f := &FSM{}

	key := []byte("cluster-fsm:test-key")
	// Genuine plaintext: no magic bytes at all
	plain := []byte(`{"key":"value"}`)

	got, err := f.openValue(key, plain)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

func TestFSMOpenValueRoundTrip(t *testing.T) {
	f := NewFSM(badgermeta.Wrap(newTestDB(t)), newStateKeyspaceEmpty())
	clusterID := bytes.Repeat([]byte{0x71}, 16)
	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x71}, encrypt.KEKSize), clusterID)
	require.NoError(t, err)
	f.SetDEKKeeper(keeper, clusterID)

	key := []byte("cluster-fsm:test-key")
	plain := []byte("mutation body")

	sealed, err := f.sealValue(key, plain)
	require.NoError(t, err)

	got, err := f.openValue(key, sealed)
	require.NoError(t, err)
	require.Equal(t, plain, got)
}

func TestDistributedBackendSetShardServicePropagatesDEKKeeperToFSM(t *testing.T) {
	db := newTestDB(t)
	fsm := NewFSM(badgermeta.Wrap(db), newStateKeyspaceEmpty())
	backend := &DistributedBackend{store: badgermeta.Wrap(db), fsm: fsm}

	kek := bytes.Repeat([]byte{0x41}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0x42}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)

	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID))

	backend.SetShardService(svc, []string{"self"})

	require.NotNil(t, fsm.dataEncryptor(), "data-group FSM should use the DEKKeeper-backed seam")
}

func TestFSMSealValueUsesDEKFrame(t *testing.T) {
	fsm := NewFSM(badgermeta.Wrap(newTestDB(t)), newStateKeyspaceEmpty())
	kek := bytes.Repeat([]byte{0x51}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0x52}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	fsm.SetDEKKeeper(keeper, clusterID)

	key := []byte("obj:bucket/key")
	sealed, err := fsm.sealValue(key, []byte("metadata-secret"))
	require.NoError(t, err)
	require.True(t, bytes.HasPrefix(sealed, []byte("GFMV\x02")))
	require.NotContains(t, string(sealed), "metadata-secret")

	got, err := fsm.openValue(key, sealed)
	require.NoError(t, err)
	require.Equal(t, []byte("metadata-secret"), got)

	_, err = fsm.openValue([]byte("obj:other/key"), sealed)
	require.Error(t, err)
}

func TestDistributedBackendDEKFramedFSMValueSurvivesDBReopen(t *testing.T) {
	dir := t.TempDir()
	openDB := func(t *testing.T) *badger.DB {
		t.Helper()
		db, err := badger.Open(badgerutil.SmallOptions(dir))
		require.NoError(t, err)
		return db
	}

	keys := newStateKeyspaceEmpty()
	db := openDB(t)
	kek := bytes.Repeat([]byte{0x61}, encrypt.KEKSize)
	clusterID := bytes.Repeat([]byte{0x62}, 16)
	keeper, err := encrypt.NewDEKKeeper(kek, clusterID)
	require.NoError(t, err)
	fsm := NewFSM(badgermeta.Wrap(db), keys)
	backend := &DistributedBackend{store: badgermeta.Wrap(db), fsm: fsm, keys: keys}
	svc := NewShardService(t.TempDir(), nil, WithShardDEKKeeper(keeper, clusterID))
	t.Cleanup(func() { _ = svc.Close() })
	backend.SetShardService(svc, []string{"self"})

	cmd, err := EncodeCommand(CmdPutObjectMeta, PutObjectMetaCmd{
		Bucket:       "b",
		Key:          "k",
		VersionID:    "v1",
		UserMetadata: map[string]string{"x-secret": "metadata-secret"},
	})
	require.NoError(t, err)
	require.NoError(t, fsm.Apply(cmd))

	metaKey := keys.ObjectMetaKeyV("b", "k", "v1")
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey)
		require.NoError(t, err)
		raw, err := item.ValueCopy(nil)
		require.NoError(t, err)
		_, _, ok, err := decodeFSMValueFrameV2(raw)
		require.NoError(t, err)
		require.True(t, ok)
		require.NotContains(t, string(raw), "metadata-secret")
		return nil
	}))
	require.NoError(t, db.Close())

	db2 := openDB(t)
	defer db2.Close()
	fsm2 := NewFSM(badgermeta.Wrap(db2), keys)
	fsm2.SetDEKKeeper(keeper, clusterID)
	require.NoError(t, db2.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey)
		require.NoError(t, err)
		plain, err := fsm2.itemValueCopy(item)
		require.NoError(t, err)
		require.Contains(t, string(plain), "metadata-secret")
		return nil
	}))
}
