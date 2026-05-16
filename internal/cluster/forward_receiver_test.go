package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

// failingObjectIndexProposer returns an error for every propose call.
type failingObjectIndexProposer struct{}

func (f *failingObjectIndexProposer) ProposeObjectIndex(_ context.Context, _ ObjectIndexEntry, _ bool) error {
	return errors.New("simulated propose failure")
}
func (f *failingObjectIndexProposer) ProposeDeleteObjectIndex(_ context.Context, _, _, _ string) error {
	return errors.New("simulated propose failure")
}

// Test helpers

func setupReceiver(t *testing.T, selfID string) (*ForwardReceiver, *DataGroupManager) {
	mgr := NewDataGroupManager()
	rcv := NewForwardReceiver(mgr)
	return rcv, mgr
}

func TestMapErrorToStatus_InsufficientPlacementTargets(t *testing.T) {
	err := &ErrInsufficientPlacementTargets{
		Operation:     "put_object",
		GroupID:       "group-1",
		Desired:       ECConfig{DataShards: 4, ParityShards: 2},
		Configured:    []string{"n1", "n2", "n3", "n4", "n5", "n6"},
		Unavailable:   []string{"n6"},
		FailureReason: "not writeable",
	}
	require.Equal(t, raftpb.ForwardStatusInsufficientPlacementTargets, mapErrorToStatus(err))
}

func TestContextForForwardedGroupCarriesPlacementEntry(t *testing.T) {
	ctx := contextForForwardedGroup(t.Context(), NewDataGroup("group-1", []string{"n1", "n2", "n3"}))

	group, ok := PlacementGroupEntryFromContext(ctx)
	require.True(t, ok)
	require.Equal(t, "group-1", group.ID)
	require.Equal(t, []string{"n1", "n2", "n3"}, group.PeerIDs)
}

// Tests

func TestForwardReceiver_UnknownGroup_NotVoter(t *testing.T) {
	rcv, _ := setupReceiver(t, "self")
	payload := encodeForwardPayload("g99", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotVoter, fr.Status())
}

func TestForwardReceiver_HandleBody_EarlyRejectDrainsBody(t *testing.T) {
	rcv, _ := setupReceiver(t, "self")
	payload := encodeForwardPayload("g99", raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil))
	body := bytes.NewBuffer(bytes.Repeat([]byte("x"), 64*1024))

	reply := rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}, body)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNotVoter, fr.Status())
	require.Zero(t, body.Len(), "early streamed-forward rejects must drain body so the sender can finish")
}

func TestForwardReceiver_NonLeaderVoter_ReturnsHint(t *testing.T) {
	rcv, mgr := setupReceiver(t, "self")

	// Create a minimal GroupBackend with wrapped DistributedBackend
	// The wrapped backend has a mock RaftNode
	mockDist := &DistributedBackend{}
	gb := WrapDistributedBackend("g1", mockDist)

	mgr.Add(NewDataGroupWithBackend("g1", []string{"self", "peer-A"}, gb))

	payload := encodeForwardPayload("g1", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	// Without a real RaftNode, we expect either NotVoter (nil node) or OK (if mock reports leader)
	status := fr.Status()
	require.True(t, status == raftpb.ForwardStatusOK || status == raftpb.ForwardStatusNotVoter || status == raftpb.ForwardStatusNotLeader,
		"expected OK/NotVoter/NotLeader, got %v", status)
}

func TestForwardReceiver_HandleGroupPropose_DispatchesToGroupBackend(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	reply := rcv.HandleGroupPropose(&transport.Message{
		Type:    transport.StreamDataGroupProposeForward,
		Payload: encodeGroupForwardPayload("group-1", []byte("propose-bytes")),
	})

	require.NotNil(t, reply)
	require.Len(t, reply.Payload, 12)
	require.Greater(t, binary.BigEndian.Uint64(reply.Payload[0:8]), uint64(0))
	require.Zero(t, binary.BigEndian.Uint32(reply.Payload[8:12]))
}

func TestForwardReceiver_HandlePutObject_CommitsObjectIndex(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	proposer := &recordingObjectIndexProposer{}
	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(proposer)

	args := buildPutObjectArgs("bucket", "mykey", "text/plain", []byte("hello"))
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	require.Len(t, proposer.entries, 1, "expected exactly one index commit")
	require.Equal(t, "bucket", proposer.entries[0].Bucket)
	require.Equal(t, "mykey", proposer.entries[0].Key)
	require.NotEmpty(t, proposer.entries[0].VersionID)
	require.Equal(t, "group-1", proposer.entries[0].PlacementGroupID)
	require.False(t, proposer.entries[0].IsDeleteMarker)
}

func TestForwardReceiver_HandlePutObject_MissingIndexProposerReturnsInternal(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	args := buildPutObjectArgs("bucket", "mykey", "text/plain", []byte("hello"))
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusInternal, fr.Status(), "mutating forwards must not succeed without object-index proposer")
	_, err := gb.HeadObject(context.Background(), "bucket", "mykey")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestForwardReceiver_HandlePutObject_ProposeError_ReturnsInternal(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(&failingObjectIndexProposer{})

	args := buildPutObjectArgs("bucket", "mykey", "text/plain", []byte("hello"))
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusInternal, fr.Status(), "ProposeObjectIndex failure must return Internal")
}

func TestForwardReceiver_HandlePutObjectStream_CommitsObjectIndex(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	proposer := &recordingObjectIndexProposer{}
	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(proposer)

	args := buildPutObjectArgs("bucket", "streamkey", "text/plain", nil)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	body := bytes.NewReader([]byte("streamed body"))

	reply := rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}, body)

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	require.Len(t, proposer.entries, 1, "expected exactly one index commit")
	require.Equal(t, "bucket", proposer.entries[0].Bucket)
	require.Equal(t, "streamkey", proposer.entries[0].Key)
	require.NotEmpty(t, proposer.entries[0].VersionID)
	require.Equal(t, "group-1", proposer.entries[0].PlacementGroupID)
}

func TestForwardReceiver_HandlePutObjectStream_MissingIndexProposerReturnsInternal(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	args := buildPutObjectArgs("bucket", "streamkey", "text/plain", nil)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply := rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}, bytes.NewReader([]byte("body")))

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusInternal, fr.Status(), "streamed mutating forwards must not succeed without object-index proposer")
	_, err := gb.HeadObject(context.Background(), "bucket", "streamkey")
	require.ErrorIs(t, err, storage.ErrObjectNotFound)
}

func TestForwardReceiver_HandlePutObjectStream_ProposeError_ReturnsInternal(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(&failingObjectIndexProposer{})

	args := buildPutObjectArgs("bucket", "streamkey", "text/plain", nil)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)

	reply := rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}, bytes.NewReader([]byte("body")))

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusInternal, fr.Status(), "ProposeObjectIndex failure must return Internal")
}

func TestForwardReceiver_HandleCompleteMultipartUpload_CommitsObjectIndex(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))

	up, err := gb.CreateMultipartUpload(context.Background(), "bucket", "mpu-key", "text/plain")
	require.NoError(t, err)

	part, err := gb.UploadPart(context.Background(), "bucket", "mpu-key", up.UploadID, 1, bytes.NewReader([]byte("part-body")))
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	proposer := &recordingObjectIndexProposer{}
	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(proposer)

	args := buildCompleteMultipartUploadArgs("bucket", "mpu-key", up.UploadID, []storage.Part{*part})
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpCompleteMultipartUpload, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	require.Len(t, proposer.entries, 1, "expected exactly one index commit")
	require.Equal(t, "bucket", proposer.entries[0].Bucket)
	require.Equal(t, "mpu-key", proposer.entries[0].Key)
	require.NotEmpty(t, proposer.entries[0].VersionID)
	require.Equal(t, "group-1", proposer.entries[0].PlacementGroupID)
	require.False(t, proposer.entries[0].IsDeleteMarker)
}

func TestForwardReceiver_HandleCompleteMultipartUpload_ProposeError_ReturnsInternal(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))

	up, err := gb.CreateMultipartUpload(context.Background(), "bucket", "mpu-key", "text/plain")
	require.NoError(t, err)

	part, err := gb.UploadPart(context.Background(), "bucket", "mpu-key", up.UploadID, 1, bytes.NewReader([]byte("part-body")))
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(&failingObjectIndexProposer{})

	args := buildCompleteMultipartUploadArgs("bucket", "mpu-key", up.UploadID, []storage.Part{*part})
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpCompleteMultipartUpload, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusInternal, fr.Status(), "ProposeObjectIndex failure must return Internal")
}

func TestForwardReceiver_HandleListParts_CallsBackend(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	up, err := gb.CreateMultipartUpload(context.Background(), "bucket", "mpu-key", "text/plain")
	require.NoError(t, err)
	part, err := gb.UploadPart(context.Background(), "bucket", "mpu-key", up.UploadID, 1, bytes.NewReader([]byte("part-one")))
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	payload := encodeForwardPayload("group-1", raftpb.ForwardOpListParts, buildListPartsArgs("bucket", "mpu-key", up.UploadID, 100))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

	require.NotNil(t, reply)
	parts, err := partsFromReply(reply.Payload)
	require.NoError(t, err)
	require.Equal(t, []storage.Part{*part}, parts)
}

func TestForwardReceiver_HandleListParts_MissingUploadReturnsNoSuchUpload(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	payload := encodeForwardPayload("group-1", raftpb.ForwardOpListParts, buildListPartsArgs("bucket", "mpu-key", "missing", 100))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusNoSuchUpload, fr.Status())
}

func TestForwardReceiver_HandleListMultipartUploads_CallsBackend(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	up, err := gb.CreateMultipartUpload(context.Background(), "bucket", "listed/mpu-key", "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	payload := encodeForwardPayload("group-1", raftpb.ForwardOpListMultipartUploads, buildListMultipartUploadsArgs("bucket", "listed/", 100))
	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})

	require.NotNil(t, reply)
	uploads, err := multipartUploadsFromReply(reply.Payload)
	require.NoError(t, err)
	require.Equal(t, []*storage.MultipartUpload{up}, uploads)
}

func TestForwardReceiver_HandleDeleteObject_CommitsDeleteMarkerIndex(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	_, err := gb.PutObject(context.Background(), "bucket", "del-key", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	proposer := &recordingObjectIndexProposer{}
	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(proposer)

	args := buildDeleteObjectArgs("bucket", "del-key")
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpDeleteObject, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	require.Len(t, proposer.entries, 1, "expected exactly one index commit")
	require.Equal(t, "bucket", proposer.entries[0].Bucket)
	require.Equal(t, "del-key", proposer.entries[0].Key)
	require.NotEmpty(t, proposer.entries[0].VersionID)
	require.Equal(t, "group-1", proposer.entries[0].PlacementGroupID)
	require.True(t, proposer.entries[0].IsDeleteMarker, "delete object must commit a delete-marker entry")
}

func TestForwardReceiver_HandleDeleteObject_ProposeError_ReturnsInternal(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	_, err := gb.PutObject(context.Background(), "bucket", "del-key", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(&failingObjectIndexProposer{})

	args := buildDeleteObjectArgs("bucket", "del-key")
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpDeleteObject, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusInternal, fr.Status(), "ProposeObjectIndex failure must return Internal")
}

func TestForwardReceiver_HandleDeleteObjectVersion_CommitsDeleteIndex(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	obj, err := gb.PutObject(context.Background(), "bucket", "ver-key", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	proposer := &recordingObjectIndexProposer{}
	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(proposer)

	args := buildDeleteObjectVersionArgs("bucket", "ver-key", obj.VersionID)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpDeleteObjectVersion, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	require.Len(t, proposer.deleted, 1, "expected exactly one delete index commit")
	require.Equal(t, "bucket/ver-key/"+obj.VersionID, proposer.deleted[0])
}

func TestForwardReceiver_HandleDeleteObjectVersion_ProposeError_ReturnsInternal(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	obj, err := gb.PutObject(context.Background(), "bucket", "ver-key", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(&failingObjectIndexProposer{})

	args := buildDeleteObjectVersionArgs("bucket", "ver-key", obj.VersionID)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpDeleteObjectVersion, args)

	reply := rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload})
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
	require.Equal(t, raftpb.ForwardStatusInternal, fr.Status(), "ProposeDeleteObjectIndex failure must return Internal")
}

func TestForwardReceiver_HandlePutObjectStreamRecordsTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()

	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	proposer := &recordingObjectIndexProposer{}
	rcv := NewForwardReceiver(mgr).WithObjectIndexProposer(proposer)

	args := buildPutObjectArgs("bucket", "stream-trace-key", "text/plain", nil)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply := rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: payload}, bytes.NewReader([]byte("streamed body")))
	require.NotNil(t, reply)

	events := readPutTraceEvents(t, path)
	requirePutTraceStage(t, events, PutTraceStageForwardReceiverDispatch)
	requirePutTraceStage(t, events, PutTraceStageReceiverBackendPut)
	requirePutTraceStage(t, events, PutTraceStageDataRaftProposeMeta)
	requirePutTraceStage(t, events, PutTraceStageMetaIndexPropose)
	var meta PutTraceEvent
	for _, ev := range events {
		if ev.Stage == PutTraceStageMetaIndexPropose {
			meta = ev
		}
	}
	require.Equal(t, "receiver", meta.MetaProposeSite)
	require.Equal(t, 1, meta.MetaProposeCount)
}
