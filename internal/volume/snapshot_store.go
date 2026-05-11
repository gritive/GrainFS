package volume

import "context"

// SnapshotStore abstracts snapshot lifecycle. Two implementations:
//   - s3SnapshotStore: S3 live_map + snapshot namespace (default; dedup off).
//   - badgerSnapshotStore: BadgerDB vd:s: keyspace + dedup canonical refcounts (dedup on).
//
// All methods are called with Manager.mu HELD by the caller. Implementations
// are free to release per-chunk to keep mutex hold time low, but must
// re-acquire any caller-required invariants before returning.
//
// CreateSnapshot returns a fresh snapID; the implementation is responsible
// for any per-block copy/refcount work. The Manager handles Volume metadata
// (SnapshotCount, persistence) outside this interface.
type SnapshotStore interface {
	CreateSnapshot(ctx context.Context, vol *Volume) (snapID string, err error)
	ListSnapshots(ctx context.Context, name string) ([]SnapshotInfo, error)
	DeleteSnapshot(ctx context.Context, vol *Volume, snapID string) error
	// Rollback restores vol's logical block→physical mapping to the named snapshot.
	// Snapshots taken AFTER snapID remain intact (mirrors non-dedup semantics).
	Rollback(ctx context.Context, vol *Volume, snapID string) error
	// Clone creates dstName as a sibling of srcVol sharing initial state.
	// Returns the dst Volume with metadata fields populated; caller persists
	// the metadata key after this returns success.
	Clone(ctx context.Context, srcVol *Volume, dstName string) (*Volume, error)
	// RecoverOnBoot is invoked once after the Manager (and DedupIndex, if any)
	// are constructed, before serving traffic. Used by badgerSnapshotStore to
	// roll back any begun-but-not-committed snapshots from a prior crash.
	// s3SnapshotStore returns nil.
	//
	// TODO(PR-B): wire this into the serveruntime boot path
	// (internal/serveruntime/volume_manager.go BuildVolumeManager). Until then,
	// the s3 implementation is a no-op so calling it is harmless but also
	// pointless. The badger implementation in PR-B will require this wiring.
	RecoverOnBoot(ctx context.Context) error
}
