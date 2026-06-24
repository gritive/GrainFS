package cluster

import (
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/storage"
)

// CommandType identifies the type of FSM command replicated through Raft.
type CommandType uint8

const (
	// CmdNoOp is proposed by a new leader to commit entries from previous terms.
	// The FSM ignores it; it exists only to advance advanceCommitIndex.
	CmdNoOp CommandType = 0

	// RETIRED: bucket control-plane moved to meta-raft (MetaBucketStore).
	// Enum slots kept reserved so old-log replay is safe (apply is a no-op).
	// DO NOT reuse these numbers for new commands.
	CmdCreateBucket       CommandType = 1
	CmdDeleteBucket       CommandType = 2
	CmdSetBucketPolicy    CommandType = 8
	CmdDeleteBucketPolicy CommandType = 9
	// CmdSetBucketVersioning (15) is also retired — kept below for wire stability.

	CmdMigrateShard  CommandType = 10
	CmdMigrationDone CommandType = 11

	// RETIRED: bucket versioning moved to meta-raft. Slot kept reserved.
	CmdSetBucketVersioning CommandType = 15
	CmdSetRing             CommandType = 17
	// CmdResealFSMValues re-seals a batch of data-group FSM state values
	// (policy:, obj:) from a retired DEK generation onto the active generation.
	// Applied in the serialized apply loop for race-freedom. S7-1a.
	CmdResealFSMValues CommandType = 41
	// CmdFSMValueResealDone is an ordering-fence marker proposed by the group
	// leader after DrainFSMValueRewrap returns nil. Every node applies the
	// marker after all preceding CmdResealFSMValues batches (raft ordering),
	// so the per-node post-apply hook fires with the node's store already
	// clean. Gen is a log hint only — the re-Kick is gen-agnostic. S7-1a-2.
	CmdFSMValueResealDone CommandType = 42

	// RETIRED SLOTS — these CommandType values once named per-object, multipart,
	// append/coalesce, and ring-derived placement commands. The data plane moved
	// off-raft (Slices 0-2 + multipart/append-off-raft epics), so these commands
	// have no production proposer and their applies were no-ops. The named
	// constants are removed; the explicit numeric values above are unchanged so
	// the LIVE control-plane commands keep their byte-stable wire contract.
	// Retired slot numbers — DO NOT REUSE for a new command:
	//   3  CmdPutObjectMeta        (off-raft quorum-meta blob; see encodeQuorumMetaBlob)
	//   4  CmdDeleteObject         (blob-physical force-delete)
	//   5  CmdCreateMultipartUpload
	//   6  CmdCompleteMultipart
	//   7  CmdAbortMultipart
	//   12 CmdPutShardPlacement    (ring-derived placement)
	//   13 CmdDeleteShardPlacement (ring-derived placement)
	//   14 CmdDeleteObjectVersion  (per-version blob tombstone)
	//   16 CmdSetObjectACL         (blob RMW SetObjectACLPropose)
	//   18 CmdAppendObject         (append-off-raft Slice 1)
	//   19 CmdCoalesceSegments     (append-off-raft Slice 1)
	//   20 CmdSetObjectTags        (blob RMW SetObjectTagsPropose)
	//   40 CmdPutObjectQuarantine  (folded into quorum-meta blob)
	//   43 CmdDeleteMultipartDone
	//   44 CmdSetBucketSoleAuthority (soleauth teardown, blob-primary epic)
)

// Command is a serializable FSM command for Raft log entries.
type Command struct {
	Type CommandType
	Data []byte
}

type CreateBucketCmd struct {
	Bucket         string
	BypassReserved bool
}

type DeleteBucketCmd struct {
	Bucket string
}

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
	// PlacementGroupID is the data Raft group that owns this object version.
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

// virtualNode는 SetRingCmd 역직렬화 경로에서 사용되는 레거시 타입이다.
// SetRingCmd.applySetRing은 no-op stub — 레거시 Raft 로그 재생 호환용.
type virtualNode struct {
	Token  uint32
	NodeID string
}

// SetRingCmd는 컨시스턴트 해시 링을 FSM에 커밋하는 명령이다.
// 멤버십 변경 시에만 propose됐다. applySetRing은 no-op stub — 레거시 Raft 로그 재생용.
type SetRingCmd struct {
	Version  uint64
	VNodes   []virtualNode
	VPerNode int
}

type PutObjectQuarantineCmd struct {
	Bucket    string
	Key       string
	VersionID string
	Cause     string
	Reason    string
}

// ResealFSMValuesCmd carries a batch of full storage keys whose FSM-state values
// (policy:, obj:) must be re-sealed onto ActiveGen. Applied in the serialized
// apply loop; the apply handler reads the current value and reseals it — never
// carries plaintext or ciphertext on the wire. S7-1a.
type ResealFSMValuesCmd struct {
	Keys      []string
	ActiveGen uint32
}

// FSMValueResealDoneCmd is the ordering-fence marker for CmdFSMValueResealDone.
// Gen is a log hint for observability only; the re-Kick triggered by the
// post-apply hook is gen-agnostic. S7-1a-2.
type FSMValueResealDoneCmd struct {
	Gen uint32
}

// DeleteMultipartDoneCmd, CreateMultipartUploadCmd, CompleteMultipartCmd, and
// AbortMultipartCmd are removed in the multipart-off-raft epic (M4).
// The CommandType constants (5, 6, 7, 43) are kept reserved above; the structs
// are deleted because no production code constructs or decodes them.

type SetBucketPolicyCmd struct {
	Bucket     string
	PolicyJSON []byte
}

type DeleteBucketPolicyCmd struct {
	Bucket string
}

// MigrateShardFSMCmd is the FSM-level command requesting a shard migration.
type MigrateShardFSMCmd struct {
	Bucket    string
	Key       string
	VersionID string
	SrcNode   string
	DstNode   string
}

// MigrationDoneFSMCmd is the FSM-level command confirming a migration is complete.
type MigrationDoneFSMCmd struct {
	Bucket    string
	Key       string
	VersionID string
	SrcNode   string
	DstNode   string
}

// SetBucketVersioningCmd persists the S3 versioning state for a bucket.
type SetBucketVersioningCmd struct {
	Bucket string
	State  string // "Enabled" | "Suspended"
}

// AppendObjectCmd records one appended segment. Only PlacementGroupID is
// frozen at propose time — FSM apply derives NodeIDs/EC params from the
// current ShardGroup record (single source of truth, avoids drift).
type AppendObjectCmd struct {
	Bucket           string
	Key              string
	ExpectedOffset   int64
	BlobID           string
	SegmentSize      int64
	SegmentETag      string
	PlacementGroupID string
	// VersionID is frozen into the Raft log so all replicas write the same
	// versioned key. AppendObject reuses the current latest version after the
	// first segment because versioning-enabled buckets reject AppendObject.
	// When empty (legacy replay / direct apply-test fixtures), apply falls back
	// to the legacy ObjectMetaKey-only single-write path.
	VersionID       string
	ModifiedUnixSec int64
}

// EncodeNoOpCommand returns a serialized CmdNoOp suitable for SetNoOpCommand.
func EncodeNoOpCommand() ([]byte, error) {
	return EncodeCommand(CmdNoOp, nil)
}

// EncodeCommand serializes a command for Raft proposal using FlatBuffers.
func EncodeCommand(cmdType CommandType, payload any) ([]byte, error) {
	data, err := encodePayload(cmdType, payload)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	b := flatbuffers.NewBuilder(len(data) + 16)
	var dataOff flatbuffers.UOffsetT
	if len(data) > 0 {
		dataOff = b.CreateByteVector(data)
	}
	clusterpb.CommandStart(b)
	clusterpb.CommandAddType(b, uint32(cmdType))
	if len(data) > 0 {
		clusterpb.CommandAddData(b, dataOff)
	}
	root := clusterpb.CommandEnd(b)
	b.Finish(root)
	raw := b.FinishedBytes()
	out := make([]byte, len(raw))
	copy(out, raw)
	return out, nil
}

// DecodeCommand deserializes a command from a Raft log entry using FlatBuffers.
func DecodeCommand(raw []byte) (cmd *Command, err error) {
	if len(raw) == 0 {
		return nil, fmt.Errorf("unmarshal command: empty data")
	}
	defer func() {
		if r := recover(); r != nil {
			cmd = nil
			err = fmt.Errorf("unmarshal command: invalid flatbuffer: %v", r)
		}
	}()
	t := clusterpb.GetRootAsCommand(raw, 0)
	return &Command{
		Type: CommandType(t.Type()),
		Data: t.DataBytes(),
	}, nil
}
