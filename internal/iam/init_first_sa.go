package iam

import (
	"context"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/iam/iampb"
)

// DefaultSAID is the well-known sa_id used by the admin-UDS-triggered
// first-SA bootstrap. Stable across concurrent proposes so the race
// guard collapses to a single SA via FSM idempotency on (sa_id) and on
// duplicate KeyCreate for the same access_key.
const DefaultSAID = "sa-default"

// Proposer abstracts the meta-FSM Propose interface so admin API
// handlers can be unit-tested without raft. Each method must propose
// the corresponding MetaCmd payload through Raft and return only after
// the command has been committed (not just enqueued).
//
// NOTE (Task 2): interface is relocated verbatim from bootstrap.go.
// Task 3 swaps ProposeAuthEnable for ProposeInitFirstSA together with
// the proposer_cluster.go + admin_api.go rewrites to keep the package
// compiling at every commit boundary.
type Proposer interface {
	ProposeSACreate(ctx context.Context, sa ServiceAccount) error
	ProposeSADelete(ctx context.Context, saID string) error
	ProposeKeyCreate(ctx context.Context, k AccessKey) error
	ProposeKeyCreateScoped(ctx context.Context, k AccessKey) error
	ProposeKeyRevoke(ctx context.Context, accessKey string) error
	ProposeGrantPut(ctx context.Context, g Grant) error
	ProposeGrantDelete(ctx context.Context, saID, bucket string) error
	ProposeGrantWildcardPut(ctx context.Context, g Grant) error
	ProposeGrantWildcardDelete(ctx context.Context, saID string) error
	ProposeAuthEnable(ctx context.Context) error
}

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
