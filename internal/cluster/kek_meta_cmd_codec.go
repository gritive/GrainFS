package cluster

import (
	"errors"
	"fmt"

	flatbuffers "github.com/google/flatbuffers/go"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
)

const currentKEKRotatePayloadVersion uint8 = 1
const currentKEKRetirePayloadVersion uint8 = 1

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
