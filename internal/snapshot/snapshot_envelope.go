package snapshot

import (
	"bytes"
	"fmt"

	"github.com/google/uuid"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/metrics"
)

// KEKSource is the minimal KEK accessor the snapshot Manager needs to seal and
// open envelopes. *encrypt.KEKStore satisfies it. Defined at the use site so
// tests can supply a trivial fake.
type KEKSource interface {
	ActiveVersion() uint32
	Get(version uint32) ([]byte, error)
}

// sealSnapshotBlob wraps a plaintext framed snapshot blob in a per-snapshot
// ephemeral-DEK + KEK envelope (Phase D-snap Slice 2). It mints a fresh UUIDv7
// snapshot id and seals under the active KEK version.
func (m *Manager) sealSnapshotBlob(body []byte) ([]byte, error) {
	if m.kek == nil {
		return nil, fmt.Errorf("snapshot: write: KEK not wired")
	}
	kekVer := m.kek.ActiveVersion()
	kek, err := m.kek.Get(kekVer)
	if err != nil {
		return nil, fmt.Errorf("snapshot: write: active KEK v%d: %w", kekVer, err)
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("snapshot: write: snapshot id: %w", err)
	}
	var sid [16]byte
	copy(sid[:], id[:])
	return encrypt.SealSnapshotEnvelope(kek, m.clusterID[:], sid, kekVer, body)
}

// openSnapshotBlob reverses sealSnapshotBlob. A legacy pre-Slice-2 snapshot has
// no envelope magic; if it carries the GFSNAP01 framing it is read as plaintext
// (read-compat migration window; D-cut removes this). Anything that is neither an
// envelope nor a GFSNAP01 file is rejected. The KEK version recorded in the
// plaintext header is resolved from the source so restore survives a
// post-snapshot KEK rotation (subject to that version still being loaded).
func (m *Manager) openSnapshotBlob(data []byte) ([]byte, error) {
	if !encrypt.IsSnapshotEnvelope(data) {
		if !bytes.HasPrefix(data, snapshotMagic[:]) {
			return nil, fmt.Errorf("snapshot: read: not an envelope and not a legacy GFSNAP01 file")
		}
		metrics.SnapshotLegacyPlaintextReadsTotal.Inc()
		return data, nil
	}
	if m.kek == nil {
		return nil, fmt.Errorf("snapshot: read: KEK not wired")
	}
	hdr, _, _, err := encrypt.PeekSnapshotEnvelopeHeader(data)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read: %w", err)
	}
	kek, err := m.kek.Get(hdr.ActiveKEKVersion())
	if err != nil {
		return nil, fmt.Errorf("snapshot: read: resolve KEK v%d: %w", hdr.ActiveKEKVersion(), err)
	}
	_, body, err := encrypt.OpenSnapshotEnvelope(kek, m.clusterID[:], data)
	if err != nil {
		return nil, fmt.Errorf("snapshot: read: open snapshot envelope: %w", err)
	}
	return body, nil
}
