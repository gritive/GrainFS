package cluster

import "github.com/gritive/GrainFS/internal/storage"

type PutObjectMetaCmd struct {
	Bucket      string
	Key         string
	Size        int64
	ContentType string
	ETag        string
	ModTime     int64
	VersionID   string
	ECData      uint8    // EC k (data shards)
	ECParity    uint8    // EC m (parity shards)
	StripeBytes uint32   // 0 = contiguous/legacy, >0 = stripe-interleaved chunk size
	NodeIDs     []string // EC 샤드 배치 노드 (index i = shard i); N× 오브젝트는 빈 슬라이스
	// PlacementGroupID is the placement group that owns this object version.
	PlacementGroupID string
	UserMetadata     map[string]string
	SSEAlgorithm     string
	// ExpectedETag makes the metadata write conditional on the latest object
	// still matching this ETag. Empty means unconditional.
	ExpectedETag string
	// PreserveLatest writes this version without moving lat:{bucket}/{key}.
	// Snapshot restore uses it for non-latest versions.
	PreserveLatest bool
	IsDeleteMarker bool
	// Parts carries the multipart parts list when this command commits a
	// CompleteMultipartUpload object; nil for ordinary PUT and AppendObject
	// commits. Replicas write the slice onto ObjectIndexEntry.Parts so any
	// node can answer GET ?partNumber=N without re-reading the body.
	Parts []storage.MultipartPartEntry
	// Segments carries per-segment placement metadata for chunked PUTs
	// (sp.Size > storage.DefaultChunkSize). Empty/nil for legacy single-segment,
	// appendable, and multipart objects. When non-empty, top-level
	// NodeIDs/ECData/ECParity/PlacementGroupID mirror Segments[0] for
	// backward-compatible single-segment readers; per-segment placements live
	// in Segments[].
	Segments []SegmentMetaEntry
	// Tags carries tagging metadata to materialise onto objectMeta.Tags at
	// finalisation. Currently populated on the CompleteMultipartUpload path
	// (carried from clusterMultipartMeta.Tags) so cluster reaches single-node
	// parity for CreateMultipartUploadWithTags.
	Tags []storage.Tag
	// ACL is the s3auth.ACLGrant bitmask. 0 = private (backward-compatible
	// default; old encoded blobs decode to 0).
	ACL uint8
	// MetaSeq is the lowest-priority quorum-meta LWW tiebreak. When two blobs
	// have an equal (ModTime, VersionID), the higher MetaSeq wins. Genuine
	// client writes always differ in ModTime/VersionID, so MetaSeq is never
	// consulted for them; it exists for placement re-writes (object relocation)
	// that preserve the original ModTime AND VersionID. Default 0 = original
	// write (behavior-neutral until the relocation feature writes >0).
	MetaSeq uint64
	// IsHardDeleted marks this per-version blob as a hard-delete TOMBSTONE under
	// blob-primary: the version is permanently deleted (distinct from
	// IsDeleteMarker, a soft-delete marker version). A tombstone replaces the data
	// blob at the same {key}/{vid} path, wins the LWW tie (quorumMetaCmdWins), is
	// excluded from every read (→404), and is reconciled/GC'd by the orphan walker.
	// Default false = a normal data blob (old blobs decode false).
	IsHardDeleted bool
	// Coalesced carries coalesced blob refs for appendable objects (Slice 1).
	// Empty/nil for non-appendable objects (backward-compatible default).
	Coalesced []CoalescedShardRef
	// IsAppendable indicates the object was created via AppendObject (Slice 1).
	// Read path uses this to dispatch the appendable reader.
	IsAppendable bool
	// MetaSeqCAS signals per-write CAS intent (Slice 1). When true the
	// quorum-meta write-time guard requires existing.MetaSeq+1 == cand.MetaSeq
	// (mutable-accumulating RMW: append/coalesce). When false → LWW.
	MetaSeqCAS bool
	// IsQuarantined marks this object version as quarantined (corrupt shard or
	// other issue). Stored in the quorum-meta blob; replaces the legacy FSM
	// quarantine: key (Slice 2). Default false = not quarantined.
	IsQuarantined bool
	// QuarantineCause is the cause string set at quarantine time
	// (e.g. incident.CauseCorruptShard). Empty if not quarantined.
	QuarantineCause string
	// AppendCallMD5s is the per-AppendObject-call digest history (one entry per
	// append call; the call's segment Checksum). Persisted so the composite ETag
	// survives a coalesce, which consumes Segments. Empty/nil for non-appendable.
	AppendCallMD5s [][]byte
}

// SegmentMetaEntry records the placement of one chunked-PUT segment. The
// per-segment EC params (NodeIDs/ECData/ECParity) are required
// for Task 2.4 GETs to read segments outside the segment-0 mirror.
type SegmentMetaEntry struct {
	BlobID           string
	Size             int64
	Checksum         []byte // xxhash3-128 (16 B)
	PlacementGroupID string
	ShardSize        int32
	SegmentIdx       int32 // index in Segments[]; ensures deterministic ordering on apply
	NodeIDs          []string
	ECData           uint8
	ECParity         uint8
	StripeBytes      uint32
}

// DeleteMultipartDoneCmd, CreateMultipartUploadCmd, CompleteMultipartCmd, and
// AbortMultipartCmd are removed in the multipart-off-raft epic (M4).
// Their data-group command slots are not named in code anymore; brownfield log
// replay treats committed command entries as opaque cursor markers.
