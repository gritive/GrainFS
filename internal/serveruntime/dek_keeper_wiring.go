package serveruntime

import (
	"fmt"
	"path/filepath"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/nodeconfig"
)

// wireDEKKeeper loads the cluster KEKStore (keys/<V>.key under dataDir),
// constructs a DEK Keeper from the active KEK, builds the shared
// HandshakeVerifier bound to the persisted cluster.id, and injects the
// keeper into the FSM + registers the DEK post-commit hook.
//
// KEK load mode (§7 B3 / F#21):
//
//   - First-cluster-init mode (NOT joining, NO peers): an empty keys/
//     directory triggers auto-generation of KEK v0 by
//     LoadOrInitKEKStoreDir. This is the very first node bootstrapping
//     its own KEK.
//
//   - Join mode (`--join-pending` OR peers configured): we refuse to
//     auto-generate. A joining node MUST already hold the cluster's KEK
//     before serve can start — auto-generating a fresh random KEK here
//     would produce a node whose KEK never decrypts the cluster's
//     FSM-wrapped DEKs. The pre-flight checks keys/ contents BEFORE
//     calling LoadOrInitKEKStoreDir.
//
// cluster.id (Phase A): generated as UUID v7 on first boot, persisted to
// <dataDir>/cluster.id, and loaded on subsequent boots. Joiners receive
// the file out-of-band (operator scp's it alongside the active KEK).
func wireDEKKeeper(state *bootState, fsm *cluster.MetaFSM) error {
	cfg := nodeconfig.New(state.cfg.DataDir)
	keysDir := cfg.KEKDir()

	// Disk-space pre-flight: refuse boot if the keystore filesystem
	// can't fit a rotation. Probes the parent so we don't fail when the
	// keys/ subdir doesn't exist yet on first boot.
	if err := CheckKeystoreDiskSpace(filepath.Dir(keysDir), MinKeystoreFreeBytes); err != nil {
		return fmt.Errorf("wireDEKKeeper: %w", err)
	}

	if state.joinMode || len(state.peers) > 0 {
		if empty, err := encrypt.KeysDirIsEmpty(keysDir); err != nil {
			return fmt.Errorf("wireDEKKeeper: stat keys dir %s: %w", keysDir, err)
		} else if empty {
			return fmt.Errorf("KEK not found at %s and this node is configured to join an existing cluster. "+
				"A joining node MUST already hold the cluster's KEK before serve can start — "+
				"auto-generating a fresh KEK here would produce a node whose KEK never decrypts "+
				"the cluster's FSM-wrapped DEKs. Options: "+
				"(a) restore the cluster KEK by scp from any healthy peer "+
				"(the KEK files are identical on every cluster node), "+
				"(b) use `grainfs cluster join <peer>` from the joining node to complete the "+
				"offline-bootstrap handshake (still requires the KEK in place locally), "+
				"(c) decommission this node and rejoin from scratch (loses local raft state).",
				keysDir)
		}
	}

	store, err := encrypt.LoadOrInitKEKStoreDir(keysDir)
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: load keystore %s: %w", keysDir, err)
	}

	activeKEK, err := store.ActiveKEK()
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: active KEK: %w", err)
	}
	defer zeroizeKEKCopy(activeKEK)

	keeper, err := encrypt.NewDEKKeeper(activeKEK)
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: init DEK keeper: %w", err)
	}

	var clusterID []byte
	if state.joinMode || len(state.peers) > 0 {
		clusterID, err = cfg.LoadClusterID()
	} else {
		clusterID, err = cfg.LoadOrInitClusterID()
	}
	if err != nil {
		return fmt.Errorf("wireDEKKeeper: cluster.id: %w", err)
	}

	fsm.SetDEKKeeper(keeper)
	state.dekKeeper = keeper
	state.kekStore = store
	state.handshakeVerifier = encrypt.NewHandshakeVerifier(store, clusterID)
	WireDEKPostCommit(fsm, nil /* proposer (§6) */, keeper, nil /* scrubberKick (§6) */)
	return nil
}

func zeroizeKEKCopy(b []byte) {
	for i := range b {
		b[i] = 0
	}
}
