package cluster

import (
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

const currentKEKRotatePayloadVersion uint8 = 1
const currentKEKRetirePayloadVersion uint8 = 1
const currentKEKPrunePayloadVersion uint8 = 1

// RewrappedDEKEntry holds a single DEK generation re-wrapped under the new KEK.
type RewrappedDEKEntry struct {
	Gen     uint32
	Wrapped []byte
}

// ClusterStateAtPropose captures the cluster KEK state at proposal time for
// determinism and idempotent replay in the FSM apply path.
type ClusterStateAtPropose struct {
	ActiveKEKVersion uint32
	RetainedKEKCount uint32
	LiveDEKGenCount  uint32
}

// KEKRotateCmd is the decoded in-memory form of a MetaKEKRotateCmd payload.
type KEKRotateCmd struct {
	PayloadVersion        uint8
	NewVersion            uint32
	WrappedNewKEK         []byte
	WrapSetHash           []byte
	RewrappedDEKs         []RewrappedDEKEntry
	Confirm               string
	Actor                 string
	RequestID             [16]byte
	RequestedAtUnixNanos  int64
	ClusterStateAtPropose ClusterStateAtPropose
}

// EncodeMetaKEKRotateCmd serializes a KEKRotateCmd to a FlatBuffers byte slice.
// wrap_set_hash must be exactly 32 bytes. PayloadVersion = 0 is normalized to
// currentKEKRotatePayloadVersion before encoding.
func EncodeMetaKEKRotateCmd(cmd KEKRotateCmd) ([]byte, error) {
	if cmd.PayloadVersion == 0 {
		cmd.PayloadVersion = currentKEKRotatePayloadVersion
	}
	if len(cmd.WrapSetHash) != 32 {
		return nil, fmt.Errorf("kek_meta_cmd_codec: wrap_set_hash must be 32 bytes, got %d", len(cmd.WrapSetHash))
	}

	b := clusterBuilderPool.Get()

	// All offsets (strings, byte vectors, nested tables, vectors of tables)
	// must be created before opening the outermost table.

	// 1. Scalar byte vectors and strings.
	confirmOff := b.CreateString(cmd.Confirm)
	actorOff := b.CreateString(cmd.Actor)
	wrappedNewKEKOff := b.CreateByteVector(cmd.WrappedNewKEK)
	wrapSetHashOff := b.CreateByteVector(cmd.WrapSetHash)
	requestIDOff := b.CreateByteVector(cmd.RequestID[:])

	// 2. Build RewrappedDEKEntry table offsets (reverse order per FlatBuffers convention).
	dekOffs := make([]flatbuffers.UOffsetT, len(cmd.RewrappedDEKs))
	for i := len(cmd.RewrappedDEKs) - 1; i >= 0; i-- {
		entry := cmd.RewrappedDEKs[i]
		wrappedOff := b.CreateByteVector(entry.Wrapped)
		clusterpb.RewrappedDEKEntryStart(b)
		clusterpb.RewrappedDEKEntryAddGen(b, entry.Gen)
		clusterpb.RewrappedDEKEntryAddWrapped(b, wrappedOff)
		dekOffs[i] = clusterpb.RewrappedDEKEntryEnd(b)
	}

	// 3. Build [RewrappedDEKEntry] vector.
	clusterpb.MetaKEKRotateCmdStartRewrappedDeksVector(b, len(dekOffs))
	for i := len(dekOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(dekOffs[i])
	}
	rewrappedDeksVec := b.EndVector(len(dekOffs))

	// 4. Build ClusterStateAtPropose nested table.
	clusterpb.ClusterStateAtProposeStart(b)
	clusterpb.ClusterStateAtProposeAddActiveKekVersion(b, cmd.ClusterStateAtPropose.ActiveKEKVersion)
	clusterpb.ClusterStateAtProposeAddRetainedKekCount(b, cmd.ClusterStateAtPropose.RetainedKEKCount)
	clusterpb.ClusterStateAtProposeAddLiveDekGenCount(b, cmd.ClusterStateAtPropose.LiveDEKGenCount)
	clusterStateOff := clusterpb.ClusterStateAtProposeEnd(b)

	// 5. Build the outermost MetaKEKRotateCmd table.
	clusterpb.MetaKEKRotateCmdStart(b)
	clusterpb.MetaKEKRotateCmdAddPayloadVersion(b, cmd.PayloadVersion)
	clusterpb.MetaKEKRotateCmdAddNewVersion(b, cmd.NewVersion)
	clusterpb.MetaKEKRotateCmdAddWrappedNewKek(b, wrappedNewKEKOff)
	clusterpb.MetaKEKRotateCmdAddWrapSetHash(b, wrapSetHashOff)
	clusterpb.MetaKEKRotateCmdAddRewrappedDeks(b, rewrappedDeksVec)
	clusterpb.MetaKEKRotateCmdAddConfirm(b, confirmOff)
	clusterpb.MetaKEKRotateCmdAddActor(b, actorOff)
	clusterpb.MetaKEKRotateCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaKEKRotateCmdAddRequestedAtUnixNanos(b, cmd.RequestedAtUnixNanos)
	clusterpb.MetaKEKRotateCmdAddClusterStateAtPropose(b, clusterStateOff)
	return fbFinish(b, clusterpb.MetaKEKRotateCmdEnd(b)), nil
}

// DecodeMetaKEKRotateCmd parses a FlatBuffers-encoded MetaKEKRotateCmd payload.
// Returns errors for empty/truncated data, unsupported payload_version, and
// invalid field lengths (request_id != 16, wrap_set_hash != 32).
func DecodeMetaKEKRotateCmd(data []byte) (KEKRotateCmd, error) {
	if len(data) == 0 {
		return KEKRotateCmd{}, errors.New("kek_meta_cmd_codec: MetaKEKRotateCmd: empty payload")
	}
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaKEKRotateCmd {
		return clusterpb.GetRootAsMetaKEKRotateCmd(d, 0)
	})
	if err != nil {
		return KEKRotateCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKRotateCmd: %w", err)
	}

	pv := t.PayloadVersion()
	if pv != currentKEKRotatePayloadVersion {
		return KEKRotateCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKRotateCmd: unsupported payload_version %d (want %d)", pv, currentKEKRotatePayloadVersion)
	}

	rawRequestID := t.RequestIdBytes()
	if len(rawRequestID) != 16 {
		return KEKRotateCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKRotateCmd: request_id must be 16 bytes, got %d", len(rawRequestID))
	}
	var requestID [16]byte
	copy(requestID[:], rawRequestID)

	rawWrapSetHash := t.WrapSetHashBytes()
	if len(rawWrapSetHash) != 32 {
		return KEKRotateCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKRotateCmd: wrap_set_hash must be 32 bytes, got %d", len(rawWrapSetHash))
	}

	deks := make([]RewrappedDEKEntry, t.RewrappedDeksLength())
	var dekEntry clusterpb.RewrappedDEKEntry
	for i := range deks {
		if t.RewrappedDeks(&dekEntry, i) {
			deks[i] = RewrappedDEKEntry{
				Gen:     dekEntry.Gen(),
				Wrapped: append([]byte(nil), dekEntry.WrappedBytes()...),
			}
		}
	}

	var clusterState ClusterStateAtPropose
	if cs := t.ClusterStateAtPropose(nil); cs != nil {
		clusterState = ClusterStateAtPropose{
			ActiveKEKVersion: cs.ActiveKekVersion(),
			RetainedKEKCount: cs.RetainedKekCount(),
			LiveDEKGenCount:  cs.LiveDekGenCount(),
		}
	}

	return KEKRotateCmd{
		PayloadVersion:        pv,
		NewVersion:            t.NewVersion(),
		WrappedNewKEK:         append([]byte(nil), t.WrappedNewKekBytes()...),
		WrapSetHash:           append([]byte(nil), rawWrapSetHash...),
		RewrappedDEKs:         deks,
		Confirm:               string(t.Confirm()),
		Actor:                 string(t.Actor()),
		RequestID:             requestID,
		RequestedAtUnixNanos:  t.RequestedAtUnixNanos(),
		ClusterStateAtPropose: clusterState,
	}, nil
}

// KEKRetireCmd is the decoded in-memory form of a MetaKEKRetireCmd payload.
type KEKRetireCmd struct {
	PayloadVersion        uint8
	Version               uint32
	Confirm               string
	Actor                 string
	RequestID             [16]byte
	RequestedAtUnixNanos  int64
	ClusterStateAtPropose ClusterStateAtPropose
}

// EncodeMetaKEKRetireCmd serializes a KEKRetireCmd to a FlatBuffers byte slice.
// PayloadVersion = 0 is normalized to currentKEKRetirePayloadVersion before encoding.
func EncodeMetaKEKRetireCmd(cmd KEKRetireCmd) ([]byte, error) {
	if cmd.PayloadVersion == 0 {
		cmd.PayloadVersion = currentKEKRetirePayloadVersion
	}

	b := clusterBuilderPool.Get()

	confirmOff := b.CreateString(cmd.Confirm)
	actorOff := b.CreateString(cmd.Actor)
	requestIDOff := b.CreateByteVector(cmd.RequestID[:])

	clusterpb.ClusterStateAtProposeStart(b)
	clusterpb.ClusterStateAtProposeAddActiveKekVersion(b, cmd.ClusterStateAtPropose.ActiveKEKVersion)
	clusterpb.ClusterStateAtProposeAddRetainedKekCount(b, cmd.ClusterStateAtPropose.RetainedKEKCount)
	clusterpb.ClusterStateAtProposeAddLiveDekGenCount(b, cmd.ClusterStateAtPropose.LiveDEKGenCount)
	clusterStateOff := clusterpb.ClusterStateAtProposeEnd(b)

	clusterpb.MetaKEKRetireCmdStart(b)
	clusterpb.MetaKEKRetireCmdAddPayloadVersion(b, cmd.PayloadVersion)
	clusterpb.MetaKEKRetireCmdAddVersion(b, cmd.Version)
	clusterpb.MetaKEKRetireCmdAddConfirm(b, confirmOff)
	clusterpb.MetaKEKRetireCmdAddActor(b, actorOff)
	clusterpb.MetaKEKRetireCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaKEKRetireCmdAddRequestedAtUnixNanos(b, cmd.RequestedAtUnixNanos)
	clusterpb.MetaKEKRetireCmdAddClusterStateAtPropose(b, clusterStateOff)
	return fbFinish(b, clusterpb.MetaKEKRetireCmdEnd(b)), nil
}

// DecodeMetaKEKRetireCmd parses a FlatBuffers-encoded MetaKEKRetireCmd payload.
// Returns errors for empty/truncated data, unsupported payload_version, and
// invalid request_id length (must be 16 bytes).
func DecodeMetaKEKRetireCmd(data []byte) (KEKRetireCmd, error) {
	if len(data) == 0 {
		return KEKRetireCmd{}, errors.New("kek_meta_cmd_codec: MetaKEKRetireCmd: empty payload")
	}
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaKEKRetireCmd {
		return clusterpb.GetRootAsMetaKEKRetireCmd(d, 0)
	})
	if err != nil {
		return KEKRetireCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKRetireCmd: %w", err)
	}

	pv := t.PayloadVersion()
	if pv != currentKEKRetirePayloadVersion {
		return KEKRetireCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKRetireCmd: unsupported payload_version %d (want %d)", pv, currentKEKRetirePayloadVersion)
	}

	rawRequestID := t.RequestIdBytes()
	if len(rawRequestID) != 16 {
		return KEKRetireCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKRetireCmd: request_id must be 16 bytes, got %d", len(rawRequestID))
	}
	var requestID [16]byte
	copy(requestID[:], rawRequestID)

	var clusterState ClusterStateAtPropose
	if cs := t.ClusterStateAtPropose(nil); cs != nil {
		clusterState = ClusterStateAtPropose{
			ActiveKEKVersion: cs.ActiveKekVersion(),
			RetainedKEKCount: cs.RetainedKekCount(),
			LiveDEKGenCount:  cs.LiveDekGenCount(),
		}
	}

	return KEKRetireCmd{
		PayloadVersion:        pv,
		Version:               t.Version(),
		Confirm:               string(t.Confirm()),
		Actor:                 string(t.Actor()),
		RequestID:             requestID,
		RequestedAtUnixNanos:  t.RequestedAtUnixNanos(),
		ClusterStateAtPropose: clusterState,
	}, nil
}

// LeaseAttestationEntry is one voter's attestation that lease_count for the
// target KEK version is zero, observed at the named raft commit index. The
// FSM Apply path verifies every entry has lease_count == 0 (Pass-6 C1).
type LeaseAttestationEntry struct {
	NodeID          string
	ObservedAtIndex uint64
	LeaseCount      uint64
}

// KEKPruneCmd is the decoded in-memory form of a MetaKEKPruneCmd payload.
// The leader stamps VoterIDs (sorted ascending unique) + VoterConfigHash;
// the FSM Apply path uses VoterIDs directly (no live raft read — determinism
// is mandatory) and cross-checks the canonical encoding against
// VoterConfigHash as a defense-in-depth gate.
type KEKPruneCmd struct {
	PayloadVersion        uint8
	Version               uint32
	Confirm               string
	LeaseAttestation      []LeaseAttestationEntry
	VoterIDs              []string
	VoterConfigIndex      uint64
	VoterConfigHash       []byte
	Actor                 string
	RequestID             [16]byte
	RequestedAtUnixNanos  int64
	ClusterStateAtPropose ClusterStateAtPropose
}

// EncodeMetaKEKPruneCmd serializes a KEKPruneCmd to a FlatBuffers byte slice.
// Validates structural invariants the FSM will require on decode: VoterIDs
// sorted ascending unique, VoterConfigHash exactly 32 bytes. PayloadVersion=0
// is normalized to currentKEKPrunePayloadVersion.
func EncodeMetaKEKPruneCmd(cmd KEKPruneCmd) ([]byte, error) {
	if cmd.PayloadVersion == 0 {
		cmd.PayloadVersion = currentKEKPrunePayloadVersion
	}
	if len(cmd.VoterConfigHash) != 32 {
		return nil, fmt.Errorf("kek_meta_cmd_codec: voter_config_hash must be 32 bytes, got %d", len(cmd.VoterConfigHash))
	}
	for i := 1; i < len(cmd.VoterIDs); i++ {
		if cmd.VoterIDs[i] <= cmd.VoterIDs[i-1] {
			return nil, fmt.Errorf("kek_meta_cmd_codec: voter_ids not sorted ascending unique at index %d (%q,%q)", i, cmd.VoterIDs[i-1], cmd.VoterIDs[i])
		}
	}

	b := clusterBuilderPool.Get()

	// Strings + byte vectors first.
	confirmOff := b.CreateString(cmd.Confirm)
	actorOff := b.CreateString(cmd.Actor)
	requestIDOff := b.CreateByteVector(cmd.RequestID[:])
	voterHashOff := b.CreateByteVector(cmd.VoterConfigHash)

	// LeaseAttestationEntry table offsets, reverse-built.
	leaseOffs := make([]flatbuffers.UOffsetT, len(cmd.LeaseAttestation))
	for i := len(cmd.LeaseAttestation) - 1; i >= 0; i-- {
		entry := cmd.LeaseAttestation[i]
		nodeOff := b.CreateString(entry.NodeID)
		clusterpb.LeaseAttestationEntryStart(b)
		clusterpb.LeaseAttestationEntryAddNodeId(b, nodeOff)
		clusterpb.LeaseAttestationEntryAddObservedAtIndex(b, entry.ObservedAtIndex)
		clusterpb.LeaseAttestationEntryAddLeaseCount(b, entry.LeaseCount)
		leaseOffs[i] = clusterpb.LeaseAttestationEntryEnd(b)
	}
	clusterpb.MetaKEKPruneCmdStartLeaseAttestationVector(b, len(leaseOffs))
	for i := len(leaseOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(leaseOffs[i])
	}
	leaseVec := b.EndVector(len(leaseOffs))

	// voter_ids vector — strings created reverse order then prepended.
	voterStrOffs := make([]flatbuffers.UOffsetT, len(cmd.VoterIDs))
	for i := len(cmd.VoterIDs) - 1; i >= 0; i-- {
		voterStrOffs[i] = b.CreateString(cmd.VoterIDs[i])
	}
	clusterpb.MetaKEKPruneCmdStartVoterIdsVector(b, len(voterStrOffs))
	for i := len(voterStrOffs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(voterStrOffs[i])
	}
	voterIDsVec := b.EndVector(len(voterStrOffs))

	clusterpb.ClusterStateAtProposeStart(b)
	clusterpb.ClusterStateAtProposeAddActiveKekVersion(b, cmd.ClusterStateAtPropose.ActiveKEKVersion)
	clusterpb.ClusterStateAtProposeAddRetainedKekCount(b, cmd.ClusterStateAtPropose.RetainedKEKCount)
	clusterpb.ClusterStateAtProposeAddLiveDekGenCount(b, cmd.ClusterStateAtPropose.LiveDEKGenCount)
	clusterStateOff := clusterpb.ClusterStateAtProposeEnd(b)

	clusterpb.MetaKEKPruneCmdStart(b)
	clusterpb.MetaKEKPruneCmdAddPayloadVersion(b, cmd.PayloadVersion)
	clusterpb.MetaKEKPruneCmdAddVersion(b, cmd.Version)
	clusterpb.MetaKEKPruneCmdAddConfirm(b, confirmOff)
	clusterpb.MetaKEKPruneCmdAddLeaseAttestation(b, leaseVec)
	clusterpb.MetaKEKPruneCmdAddVoterIds(b, voterIDsVec)
	clusterpb.MetaKEKPruneCmdAddVoterConfigIndex(b, cmd.VoterConfigIndex)
	clusterpb.MetaKEKPruneCmdAddVoterConfigHash(b, voterHashOff)
	clusterpb.MetaKEKPruneCmdAddActor(b, actorOff)
	clusterpb.MetaKEKPruneCmdAddRequestId(b, requestIDOff)
	clusterpb.MetaKEKPruneCmdAddRequestedAtUnixNanos(b, cmd.RequestedAtUnixNanos)
	clusterpb.MetaKEKPruneCmdAddClusterStateAtPropose(b, clusterStateOff)
	return fbFinish(b, clusterpb.MetaKEKPruneCmdEnd(b)), nil
}

// DecodeMetaKEKPruneCmd parses a FlatBuffers-encoded MetaKEKPruneCmd payload.
// Returns errors for empty/truncated data, unsupported payload_version,
// invalid request_id length (must be 16), invalid voter_config_hash length
// (must be 32), and voter_ids not sorted ascending unique.
func DecodeMetaKEKPruneCmd(data []byte) (KEKPruneCmd, error) {
	if len(data) == 0 {
		return KEKPruneCmd{}, errors.New("kek_meta_cmd_codec: MetaKEKPruneCmd: empty payload")
	}
	t, err := fbSafe(data, func(d []byte) *clusterpb.MetaKEKPruneCmd {
		return clusterpb.GetRootAsMetaKEKPruneCmd(d, 0)
	})
	if err != nil {
		return KEKPruneCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKPruneCmd: %w", err)
	}

	pv := t.PayloadVersion()
	if pv != currentKEKPrunePayloadVersion {
		return KEKPruneCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKPruneCmd: unsupported payload_version %d (want %d)", pv, currentKEKPrunePayloadVersion)
	}

	rawRequestID := t.RequestIdBytes()
	if len(rawRequestID) != 16 {
		return KEKPruneCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKPruneCmd: request_id must be 16 bytes, got %d", len(rawRequestID))
	}
	var requestID [16]byte
	copy(requestID[:], rawRequestID)

	rawHash := t.VoterConfigHashBytes()
	if len(rawHash) != 32 {
		return KEKPruneCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKPruneCmd: voter_config_hash must be 32 bytes, got %d", len(rawHash))
	}

	leaseLen := t.LeaseAttestationLength()
	lease := make([]LeaseAttestationEntry, leaseLen)
	var leaseEntry clusterpb.LeaseAttestationEntry
	for i := 0; i < leaseLen; i++ {
		if t.LeaseAttestation(&leaseEntry, i) {
			lease[i] = LeaseAttestationEntry{
				NodeID:          string(leaseEntry.NodeId()),
				ObservedAtIndex: leaseEntry.ObservedAtIndex(),
				LeaseCount:      leaseEntry.LeaseCount(),
			}
		}
	}

	voterLen := t.VoterIdsLength()
	voters := make([]string, voterLen)
	for i := 0; i < voterLen; i++ {
		voters[i] = string(t.VoterIds(i))
	}
	// Structural integrity: voter_ids MUST be sorted ascending unique. The
	// leader stamps in this shape; non-sorted input is a malformed payload.
	for i := 1; i < len(voters); i++ {
		if voters[i] <= voters[i-1] {
			return KEKPruneCmd{}, fmt.Errorf("kek_meta_cmd_codec: MetaKEKPruneCmd: voter_ids not sorted ascending unique at index %d (%q,%q)", i, voters[i-1], voters[i])
		}
	}

	var clusterState ClusterStateAtPropose
	if cs := t.ClusterStateAtPropose(nil); cs != nil {
		clusterState = ClusterStateAtPropose{
			ActiveKEKVersion: cs.ActiveKekVersion(),
			RetainedKEKCount: cs.RetainedKekCount(),
			LiveDEKGenCount:  cs.LiveDekGenCount(),
		}
	}

	return KEKPruneCmd{
		PayloadVersion:        pv,
		Version:               t.Version(),
		Confirm:               string(t.Confirm()),
		LeaseAttestation:      lease,
		VoterIDs:              voters,
		VoterConfigIndex:      t.VoterConfigIndex(),
		VoterConfigHash:       append([]byte(nil), rawHash...),
		Actor:                 string(t.Actor()),
		RequestID:             requestID,
		RequestedAtUnixNanos:  t.RequestedAtUnixNanos(),
		ClusterStateAtPropose: clusterState,
	}, nil
}
