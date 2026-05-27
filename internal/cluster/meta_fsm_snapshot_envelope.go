package cluster

import (
	"fmt"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/encrypt"
)

// sealSnapshotEnvelope wraps a plaintext meta-FSM snapshot blob in a
// per-snapshot ephemeral-DEK + KEK envelope (Phase D-snap). It mints a fresh
// UUIDv7 snapshot id and seals under the active KEK version.
func (f *MetaFSM) sealSnapshotEnvelope(body []byte) ([]byte, error) {
	store := f.KEKStore()
	if store == nil {
		return nil, fmt.Errorf("meta_fsm: Snapshot: KEK store not wired")
	}
	kekVer := store.ActiveVersion()
	kek, err := store.Get(kekVer)
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Snapshot: active KEK v%d: %w", kekVer, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Snapshot: snapshot id: %w", err)
	}
	var sid [16]byte
	copy(sid[:], id[:])
	cid := f.ClusterID()
	return encrypt.SealSnapshotEnvelope(kek, cid[:], sid, kekVer, body)
}

// openSnapshotEnvelope reverses sealSnapshotEnvelope, resolving the KEK version
// recorded in the plaintext header from the KEK store (supports restore across
// a KEK rotation that happened after the snapshot was sealed).
func (f *MetaFSM) openSnapshotEnvelope(data []byte) ([]byte, error) {
	// Legacy read-compat (Phase D-snap migration window): a snapshot written by a
	// pre-Slice-1 binary has no envelope magic; read it as plaintext. Snapshot()
	// always seals, so the next snapshot migrates it forward. D-cut removes this
	// fallback once all snapshots are guaranteed enveloped.
	if !encrypt.IsSnapshotEnvelope(data) {
		return data, nil
	}
	store := f.KEKStore()
	if store == nil {
		return nil, fmt.Errorf("meta_fsm: Restore: KEK store not wired")
	}
	hdr, _, _, err := encrypt.PeekSnapshotEnvelopeHeader(data)
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Restore: %w", err)
	}
	kek, err := store.Get(hdr.ActiveKEKVersion())
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Restore: resolve KEK v%d: %w", hdr.ActiveKEKVersion(), err)
	}
	cid := f.ClusterID()
	_, body, err := encrypt.OpenSnapshotEnvelope(kek, cid[:], data)
	if err != nil {
		return nil, fmt.Errorf("meta_fsm: Restore: open snapshot envelope: %w", err)
	}
	return body, nil
}
