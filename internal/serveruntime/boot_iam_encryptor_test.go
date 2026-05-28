package serveruntime

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/iam"
)

// TestWireIAMEncryptor_InstallsOnBothApplierAndAdminAPI — R2 codex P0
// regression. The IAM applier and admin API hold INDEPENDENT DataEncryptor
// references. wireIAMEncryptor must install the live keeper into BOTH —
// missing either path would silently use the wrong encryptor for
// new credentials. Mirrors the production order:
//
//  1. iam.NewApplier(store, nil)          — applier holds nil encryptor
//  2. iam.NewAdminAPI(store, prop, nil)   — admin holds nil encryptor
//  3. wireDEKKeeper populates state.dekKeeper
//  4. wireIAMEncryptor(state) swaps the live DEKKeeperAdapter into BOTH
func TestWireIAMEncryptor_InstallsOnBothApplierAndAdminAPI(t *testing.T) {
	state := &bootState{}
	state.cfg.IAMStore = iam.NewStore()
	state.cfg.IAMApplier = iam.NewApplier(state.cfg.IAMStore, nil)
	state.iamAdminAPI = iam.NewAdminAPI(state.cfg.IAMStore, &iam.MetaProposer{}, nil)
	state.clusterID = bytes.Repeat([]byte{0xab}, 16)

	keeper, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0xcd}, encrypt.KEKSize), state.clusterID)
	require.NoError(t, err)
	state.dekKeeper = keeper

	require.Nil(t, state.cfg.IAMApplier.Encryptor(), "pre-wire: applier has no encryptor")
	require.Nil(t, state.iamAdminAPI.Encryptor(), "pre-wire: admin api has no encryptor")

	wireIAMEncryptor(state)

	require.NotNil(t, state.cfg.IAMApplier.Encryptor(), "post-wire: applier has DataEncryptor")
	require.NotNil(t, state.iamAdminAPI.Encryptor(), "post-wire: admin api has DataEncryptor")
}

// TestWireIAMEncryptor_IdempotentLastWriteWins — restore path may call
// wireIAMEncryptor a second time with a DIFFERENT keeper. atomic.Pointer
// last-write-wins semantics mean the LATER call overrides the former for
// both applier and admin api.
func TestWireIAMEncryptor_IdempotentLastWriteWins(t *testing.T) {
	state := &bootState{}
	state.cfg.IAMStore = iam.NewStore()
	state.cfg.IAMApplier = iam.NewApplier(state.cfg.IAMStore, nil)
	state.iamAdminAPI = iam.NewAdminAPI(state.cfg.IAMStore, &iam.MetaProposer{}, nil)
	state.clusterID = bytes.Repeat([]byte{0xab}, 16)

	fresh, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x01}, encrypt.KEKSize), state.clusterID)
	require.NoError(t, err)
	state.dekKeeper = fresh
	wireIAMEncryptor(state) // fresh-boot wire

	firstAdapter := state.cfg.IAMApplier.Encryptor()
	require.NotNil(t, firstAdapter)

	// Simulate the restore-branch reassign in dek_keeper_restore.go:91.
	restored, err := encrypt.NewDEKKeeper(bytes.Repeat([]byte{0x02}, encrypt.KEKSize), state.clusterID)
	require.NoError(t, err)
	state.dekKeeper = restored
	wireIAMEncryptor(state) // restore wire

	secondAdapter := state.cfg.IAMApplier.Encryptor()
	require.NotNil(t, secondAdapter)
	require.NotSame(t, firstAdapter, secondAdapter, "restore call must install a fresh adapter, not reuse")

	// Admin api must follow the same swap.
	require.NotSame(t, firstAdapter, state.iamAdminAPI.Encryptor(),
		"admin api must also see the restored keeper, not the fresh one")
}

// TestWireIAMEncryptor_NoOpWhenKeeperNil — defensive: a nil dekKeeper
// (test config that didn't wire DEK; or pre-wireDEKKeeper hook) is a no-op,
// not a panic.
func TestWireIAMEncryptor_NoOpWhenKeeperNil(t *testing.T) {
	state := &bootState{}
	state.cfg.IAMStore = iam.NewStore()
	state.cfg.IAMApplier = iam.NewApplier(state.cfg.IAMStore, nil)
	// state.dekKeeper intentionally nil.
	require.NotPanics(t, func() { wireIAMEncryptor(state) })
	require.Nil(t, state.cfg.IAMApplier.Encryptor())
}
