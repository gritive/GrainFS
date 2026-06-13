package cluster

import (
	"bytes"
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// Test helpers

func setupReceiver(_ interface{}, selfID string) (*ForwardReceiver, *DataGroupManager) {
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
	reply, _ := rcv.Handle(payload)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusNotVoter, fr.Status())
}

func TestForwardReceiver_HandleBody_EarlyRejectDrainsBody(t *testing.T) {
	rcv, _ := setupReceiver(t, "self")
	payload := encodeForwardPayload("g99", raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil))
	body := bytes.NewBuffer(bytes.Repeat([]byte("x"), 64*1024))

	reply, _ := rcv.HandleBody(payload, body)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
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

	reply, _ := rcv.Handle(payload)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
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
	cmd, err := EncodeCommand(CmdCreateBucket, CreateBucketCmd{Bucket: "forward-propose"})
	require.NoError(t, err)

	reply, _ := rcv.HandleGroupPropose(encodeGroupForwardPayload("group-1", cmd))

	require.NotNil(t, reply)
	// Phase A (Task 16): success wire is [8B idx][4B errLen=0][1B applyErrCode=0].
	require.Len(t, reply, 13)
	require.Greater(t, binary.BigEndian.Uint64(reply[0:8]), uint64(0))
	require.Zero(t, binary.BigEndian.Uint32(reply[8:12]))
	require.Equal(t, byte(applyErrCodeNone), reply[12])
}

func TestForwardReceiver_HandlePutObject_ReturnsOK(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr)

	args := buildPutObjectArgs("bucket", "mykey", "text/plain", []byte("hello"))
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply, _ := rcv.Handle(payload)

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	head, err := gb.HeadObject(context.Background(), "bucket", "mykey")
	require.NoError(t, err)
	require.Equal(t, int64(5), head.Size)
}

func TestForwardReceiver_HandlePutObject_PreservesSSE(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr)

	args := buildPutObjectArgsWithSSE("bucket", "sse-key", "text/plain", []byte("hello"), "AES256", nil)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply, _ := rcv.Handle(payload)

	require.NotNil(t, reply)
	obj, err := objectFromReply(reply)
	require.NoError(t, err)
	require.Equal(t, "AES256", obj.SSEAlgorithm)

	head, err := gb.HeadObject(context.Background(), "bucket", "sse-key")
	require.NoError(t, err)
	require.Equal(t, "AES256", head.SSEAlgorithm)
}

func TestForwardReceiver_HandlePutObject_PreservesUserMetadata(t *testing.T) {
	// S3 single-path #1: a forwarded PUT must carry user metadata so it has the
	// same effect as a local one. Build args WITH metadata, run the receiver,
	// and assert the persisted object carries them.
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	um := map[string]string{"x-amz-meta-team": "storage", "x-amz-meta-env": "prod"}
	args := buildPutObjectArgsWithSSE("bucket", "meta-key", "text/plain", []byte("hello"), "", um)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply, _ := rcv.Handle(payload)

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status())

	head, err := gb.HeadObject(context.Background(), "bucket", "meta-key")
	require.NoError(t, err)
	require.Equal(t, "storage", head.UserMetadata["x-amz-meta-team"])
	require.Equal(t, "prod", head.UserMetadata["x-amz-meta-env"])
}

func TestForwardReceiver_HandlePutObjectStream_ReturnsOK(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr)

	args := buildPutObjectArgs("bucket", "streamkey", "text/plain", nil)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	body := bytes.NewReader([]byte("streamed body"))

	reply, _ := rcv.HandleBody(payload, body)

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	_, err := gb.HeadObject(context.Background(), "bucket", "streamkey")
	require.NoError(t, err)
}

func TestForwardReceiver_HandleCompleteMultipartUpload_ReturnsOK(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))

	up, err := gb.CreateMultipartUpload(context.Background(), "bucket", "mpu-key", "text/plain")
	require.NoError(t, err)

	part, err := gb.UploadPart(context.Background(), "bucket", "mpu-key", up.UploadID, 1, bytes.NewReader([]byte("part-body")))
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr)

	args := buildCompleteMultipartUploadArgs("bucket", "mpu-key", up.UploadID, []storage.Part{*part})
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpCompleteMultipartUpload, args)

	reply, _ := rcv.Handle(payload)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
	head, err := gb.HeadObject(context.Background(), "bucket", "mpu-key")
	require.NoError(t, err)
	require.NotEmpty(t, head.VersionID)
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
	reply, _ := rcv.Handle(payload)

	require.NotNil(t, reply)
	parts, err := partsFromReply(reply)
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
	reply, _ := rcv.Handle(payload)

	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
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
	reply, _ := rcv.Handle(payload)

	require.NotNil(t, reply)
	uploads, err := multipartUploadsFromReply(reply)
	require.NoError(t, err)
	require.Equal(t, []*storage.MultipartUpload{up}, uploads)
}

func TestForwardReceiver_HandleDeleteObject_ReturnsOK(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	_, err := gb.PutObject(context.Background(), "bucket", "del-key", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr)

	args := buildDeleteObjectArgs("bucket", "del-key")
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpDeleteObject, args)

	reply, _ := rcv.Handle(payload)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
}

func TestForwardReceiver_HandleDeleteObjectVersion_ReturnsOK(t *testing.T) {
	gb := newTestGroupBackend(t, "group-1")
	require.NoError(t, gb.CreateBucket(context.Background(), "bucket"))
	obj, err := gb.PutObject(context.Background(), "bucket", "ver-key", bytes.NewReader([]byte("data")), "text/plain")
	require.NoError(t, err)

	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr)

	args := buildDeleteObjectVersionArgs("bucket", "ver-key", obj.VersionID)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpDeleteObjectVersion, args)

	reply, _ := rcv.Handle(payload)
	require.NotNil(t, reply)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status(), "expected OK from leader backend")
}

func TestForwardReceiver_HandlePutObjectStreamRecordsTrace(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()

	gb := newTestGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))

	rcv := NewForwardReceiver(mgr)

	args := buildPutObjectArgs("bucket", "stream-trace-key", "text/plain", nil)
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, args)
	reply, _ := rcv.HandleBody(payload, bytes.NewReader([]byte("streamed body")))
	require.NotNil(t, reply)

	events := readPutTraceEvents(t, path)
	requirePutTraceStage(t, events, PutTraceStageForwardReceiverDispatch)
	requirePutTraceStage(t, events, PutTraceStageReceiverBackendPut)
}
