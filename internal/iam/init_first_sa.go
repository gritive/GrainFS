package iam

import (
	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

// DefaultSAID is the well-known sa_id used by the admin-UDS-triggered
// first-SA bootstrap. Stable across concurrent proposes so the race
// guard collapses to a single SA via FSM idempotency on (sa_id) and on
// duplicate KeyCreate for the same access_key.
const DefaultSAID = "sa-default"

// ApplyInitFirstSA decodes a composite InitFirstSAPayload and applies
// SA + AccessKey + WildcardGrant atomically. Idempotent: if DefaultSAID
// already exists in the store, returns nil without applying anything.
// This is the race guard for concurrent admin UDS callers on an empty
// cluster — both propose, FSM serializes via Raft FIFO, second Apply
// sees existing SA and no-ops. Result: store converges to single SA.
func (a *Applier) ApplyInitFirstSA(payload []byte) error {
	if _, exists := a.store.LookupSA(DefaultSAID); exists {
		// Idempotent skip: another propose already committed.
		return nil
	}
	root := iampb.GetRootAsInitFirstSAPayload(payload, 0)

	saBlob := readByteVector(root.SaCreateBlobLength, root.SaCreateBlob)
	keyBlob := readByteVector(root.KeyCreateBlobLength, root.KeyCreateBlob)
	gwBlob := readByteVector(root.GrantWildcardBlobLength, root.GrantWildcardBlob)

	if err := a.ApplySACreate(saBlob); err != nil {
		return err
	}
	if err := a.ApplyKeyCreate(keyBlob); err != nil {
		return err
	}
	return a.ApplyGrantWildcardPut(gwBlob)
}

// readByteVector copies the FB byte-vector data into a fresh []byte.
// Reads len() first because flatbuffers indexed accessors return single
// bytes and we need a contiguous slice to feed back into ApplyXxx.
func readByteVector(lenFn func() int, byteFn func(j int) byte) []byte {
	n := lenFn()
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		out[i] = byteFn(i)
	}
	return out
}

// NewUUIDv7 returns a time-ordered UUID v7 string. Falls back to v4 if
// the v7 entropy source fails (extremely rare; v4 is still unique but
// not time-ordered).
func NewUUIDv7() string {
	id, err := uuid.NewV7()
	if err != nil {
		return uuid.NewString()
	}
	return id.String()
}
