package cluster

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// newTestMetaFSMWithKEKAndDEK creates an FSM wired with a KEKStore (K0 active),
// a DEKKeeper (gen 1 wrapped under K0), and a known plaintext DEK.
// Returns (fsm, plainDEK) so callers can verify wraps.
func newTestMetaFSMWithKEKAndDEK(t *testing.T) (*MetaFSM, []byte) {
	t.Helper()

	k0 := bytes.Repeat([]byte{0xA0}, encrypt.KEKSize)
	plainDEK := make([]byte, encrypt.DEKSize)
	if _, err := rand.Read(plainDEK); err != nil {
		t.Fatalf("rand plainDEK: %v", err)
	}

	// Cluster ID — deterministic. DEK wraps are AAD-bound to (clusterID, gen,
	// kekVer); seed gen 1 under KEK version 0.
	var clusterID [16]byte
	for i := range clusterID {
		clusterID[i] = byte(i + 1)
	}
	dekAAD := encrypt.BuildAAD(encrypt.DomainDEKFSMWrap, clusterID[:], encrypt.FieldUint32(1), encrypt.FieldUint32(0))
	wrappedDEKK0, err := encrypt.AESGCMSealWithAAD(k0, plainDEK, dekAAD)
	if err != nil {
		t.Fatalf("seal DEK under K0: %v", err)
	}

	keeper, err := encrypt.LoadFromFSM(k0, clusterID[:], map[uint32][]byte{1: wrappedDEKK0}, 0)
	if err != nil {
		t.Fatalf("LoadFromFSM: %v", err)
	}

	store := encrypt.NewKEKStore()
	if err := store.Add(0, k0); err != nil {
		t.Fatalf("seed KEKStore: %v", err)
	}

	fsm := NewMetaFSM()
	fsm.SetClusterID(clusterID[:])
	fsm.SetKEKStore(store)
	fsm.SetDEKKeeper(keeper)
	return fsm, plainDEK
}

// applyValidKEKRotateForTest applies a single valid KEK rotation to fsm,
// advancing activeKEKVersion by 1.
func applyValidKEKRotateForTest(t *testing.T, fsm *MetaFSM) error {
	t.Helper()

	// Read live state: current activeKEKVersion and all DEK wraps.
	currentVer := fsm.ActiveKEKVersion()
	newVer := currentVer + 1

	// Fetch current KEK bytes from the keystore.
	currentKEK, err := fsm.KEKStore().Get(currentVer)
	if err != nil {
		return fmt.Errorf("get current KEK v%d: %w", currentVer, err)
	}

	// Generate new KEK.
	newKEK := make([]byte, encrypt.KEKSize)
	if _, err := rand.Read(newKEK); err != nil {
		return fmt.Errorf("rand newKEK: %w", err)
	}

	// Seal newKEK under currentKEK with rotation AAD.
	var clusterID [16]byte
	for i := range clusterID {
		clusterID[i] = byte(i + 1)
	}
	aad := encrypt.BuildAAD(encrypt.DomainKEKRotate, clusterID[:], encrypt.FieldUint32(newVer))
	wrappedNewKEK, err := encrypt.AESGCMSealWithAAD(currentKEK, newKEK, aad)
	if err != nil {
		return fmt.Errorf("wrap newKEK: %w", err)
	}

	// Snapshot current DEK wraps to build rewrapped list and hash.
	dekVersions, _ := fsm.dekKeeper.VersionsAndActive()

	wrapEntries := make([]encrypt.WrapSetEntry, 0, len(dekVersions))
	rewrapped := make([]RewrappedDEKEntry, 0, len(dekVersions))
	for gen, oldWrap := range dekVersions {
		// Unseal the DEK plaintext from the old wrap (AAD bound to current ver).
		oldAAD := encrypt.BuildAAD(encrypt.DomainDEKFSMWrap, clusterID[:], encrypt.FieldUint32(gen), encrypt.FieldUint32(currentVer))
		plainDEK, err := encrypt.AESGCMOpenWithAAD(currentKEK, oldWrap, oldAAD)
		if err != nil {
			return fmt.Errorf("unseal DEK gen %d: %w", gen, err)
		}
		// Rewrap under newKEK, re-binding AAD to the new KEK version.
		newAAD := encrypt.BuildAAD(encrypt.DomainDEKFSMWrap, clusterID[:], encrypt.FieldUint32(gen), encrypt.FieldUint32(newVer))
		newWrap, err := encrypt.AESGCMSealWithAAD(newKEK, plainDEK, newAAD)
		if err != nil {
			return fmt.Errorf("rewrap DEK gen %d: %w", gen, err)
		}
		wrapEntries = append(wrapEntries, encrypt.WrapSetEntry{Gen: gen, Wrap: oldWrap})
		rewrapped = append(rewrapped, RewrappedDEKEntry{Gen: gen, Wrapped: newWrap})
	}

	hashArr := encrypt.CanonicalWrapSetHash(wrapEntries)

	var rid [16]byte
	if _, err := rand.Read(rid[:]); err != nil {
		return fmt.Errorf("rand rid: %w", err)
	}

	cmd := KEKRotateCmd{
		PayloadVersion: 1,
		NewVersion:     newVer,
		WrappedNewKEK:  wrappedNewKEK,
		WrapSetHash:    hashArr[:],
		RewrappedDEKs:  rewrapped,
		Confirm:        "rotate-now",
		Actor:          "test",
		RequestID:      rid,
		ClusterStateAtPropose: ClusterStateAtPropose{
			ActiveKEKVersion: currentVer,
			RetainedKEKCount: 1,
			LiveDEKGenCount:  uint32(len(dekVersions)),
		},
	}

	payload, err := EncodeMetaKEKRotateCmd(cmd)
	if err != nil {
		return fmt.Errorf("encode cmd: %w", err)
	}
	envelope, err := encodeMetaCmd(MetaCmdTypeKEKRotate, payload)
	if err != nil {
		return fmt.Errorf("encode envelope: %w", err)
	}
	if err := fsm.applyCmdAtIndex(envelope, uint64(newVer*10)); err != nil {
		return fmt.Errorf("Apply: %w", err)
	}
	return nil
}

// verifySnapshotAtomicForTest parses snapshot bytes, extracts (activeKEKVersion,
// dekVersions) from the DKVS trailer, and verifies each wrap unseals under
// kekStore.Get(activeKEKVersion). This is the atomicity invariant: both fields
// must come from the same locked window.
func verifySnapshotAtomicForTest(t *testing.T, snapBytes []byte, kekStore *encrypt.KEKStore) error {
	t.Helper()
	trailers, err := peelMetaSnapshotTrailers(snapBytes)
	if err != nil {
		return fmt.Errorf("peel trailers: %w", err)
	}
	if len(trailers.dekData) == 0 {
		// No DEK data → nothing to verify (pre-DEK snapshot).
		return nil
	}
	dekVersions, _, _, activeKEKVersion, err := decodeMetaDEKVersionSnapshot(trailers.dekData)
	if err != nil {
		return fmt.Errorf("decode DKVS: %w", err)
	}
	if len(dekVersions) == 0 {
		return nil
	}
	activeKEK, err := kekStore.Get(activeKEKVersion)
	if err != nil {
		return fmt.Errorf("KEKStore.Get(%d): %w", activeKEKVersion, err)
	}
	// DEK wraps are AAD-bound to (clusterID, gen, activeKEKVersion); the test
	// FSMs use the deterministic byte(i+1) clusterID.
	var clusterID [16]byte
	for i := range clusterID {
		clusterID[i] = byte(i + 1)
	}
	for gen, wrap := range dekVersions {
		aad := encrypt.BuildAAD(encrypt.DomainDEKFSMWrap, clusterID[:], encrypt.FieldUint32(gen), encrypt.FieldUint32(activeKEKVersion))
		if _, err := encrypt.AESGCMOpenWithAAD(activeKEK, wrap, aad); err != nil {
			return fmt.Errorf("wrap[gen=%d] does not unseal under KEK v%d (torn snapshot): %w",
				gen, activeKEKVersion, err)
		}
	}
	return nil
}

// TestSnapshot_KEKRotateConcurrency_NoTear verifies that concurrent KEK
// rotation Apply calls cannot produce a torn snapshot (new wraps with old
// activeKEKVersion, or vice versa). Both activeKEKVersion and dekVersions
// must be captured in the same locked window.
func TestSnapshot_KEKRotateConcurrency_NoTear(t *testing.T) {
	fsm, _ := newTestMetaFSMWithKEKAndDEK(t)

	var (
		stopApply atomic.Bool
		applyErr  atomic.Pointer[error]
		applyDone = make(chan struct{})
	)
	go func() {
		defer close(applyDone)
		for i := 0; i < 100; i++ {
			if stopApply.Load() {
				return
			}
			if err := applyValidKEKRotateForTest(t, fsm); err != nil {
				applyErr.Store(&err)
				return
			}
		}
	}()

	for i := 0; i < 100; i++ {
		snapBytes, err := fsm.Snapshot()
		if err != nil {
			t.Fatalf("snapshot %d: %v", i, err)
		}
		if err := verifySnapshotAtomicForTest(t, snapBytes, fsm.KEKStore()); err != nil {
			stopApply.Store(true)
			t.Fatalf("torn snapshot at iteration %d: %v", i, err)
		}
	}

	stopApply.Store(true)
	<-applyDone
	if p := applyErr.Load(); p != nil {
		t.Fatalf("background apply failed: %v", *p)
	}
}
