package cluster

import (
	"bytes"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
	"github.com/stretchr/testify/require"
)

func TestBucketForwardOpSpecsCoverBucketOps(t *testing.T) {
	bucketOps := []raftpb.ForwardOp{
		raftpb.ForwardOpPutObject,
		raftpb.ForwardOpGetObject,
		raftpb.ForwardOpHeadObject,
		raftpb.ForwardOpDeleteObject,
		raftpb.ForwardOpSetObjectACL,
		raftpb.ForwardOpSetObjectTags,
		raftpb.ForwardOpGetObjectTags,
		raftpb.ForwardOpListObjects,
		raftpb.ForwardOpWalkObjects,
		raftpb.ForwardOpCreateMultipartUpload,
		raftpb.ForwardOpUploadPart,
		raftpb.ForwardOpCompleteMultipartUpload,
		raftpb.ForwardOpAbortMultipartUpload,
		raftpb.ForwardOpListParts,
		raftpb.ForwardOpListMultipartUploads,
		raftpb.ForwardOpGetObjectVersion,
		raftpb.ForwardOpDeleteObjectVersion,
		raftpb.ForwardOpListObjectVersions,
		raftpb.ForwardOpReadAt,
		raftpb.ForwardOpHeadObjectVersion,
		raftpb.ForwardOpAppendObject,
	}
	require.Len(t, bucketForwardOpSpecs, len(bucketOps))
	for _, op := range bucketOps {
		spec, ok := lookupBucketForwardOpSpec(op)
		require.True(t, ok, "missing bucket forward op spec for %s", op)
		require.Equal(t, op, spec.op)
		require.Equal(t, op.String(), spec.name)
	}

	_, ok := lookupBucketForwardOpSpec(raftpb.ForwardOpScrubSessionStat)
	require.False(t, ok, "scrub session stat is node-scoped, not a bucket forward op")
}

func TestBucketForwardOpSpecsClassifyTransportKind(t *testing.T) {
	assertKind := func(op raftpb.ForwardOp, kind forwardTransportKind, mutates bool) {
		t.Helper()
		spec, ok := lookupBucketForwardOpSpec(op)
		require.True(t, ok, "missing spec for %s", op)
		require.Equal(t, kind, spec.transport)
		require.Equal(t, mutates, spec.mutates)
	}

	assertKind(raftpb.ForwardOpGetObject, forwardReadStream, false)
	assertKind(raftpb.ForwardOpGetObjectVersion, forwardReadStream, false)
	assertKind(raftpb.ForwardOpReadAt, forwardReadStream, false)
	assertKind(raftpb.ForwardOpPutObject, forwardBodyStream, true)
	assertKind(raftpb.ForwardOpUploadPart, forwardBodyStream, true)
	assertKind(raftpb.ForwardOpAppendObject, forwardBodyStream, true)

	for _, op := range []raftpb.ForwardOp{
		raftpb.ForwardOpHeadObject,
		raftpb.ForwardOpDeleteObject,
		raftpb.ForwardOpSetObjectACL,
		raftpb.ForwardOpSetObjectTags,
		raftpb.ForwardOpGetObjectTags,
		raftpb.ForwardOpListObjects,
		raftpb.ForwardOpWalkObjects,
		raftpb.ForwardOpCreateMultipartUpload,
		raftpb.ForwardOpCompleteMultipartUpload,
		raftpb.ForwardOpAbortMultipartUpload,
		raftpb.ForwardOpListParts,
		raftpb.ForwardOpListMultipartUploads,
		raftpb.ForwardOpDeleteObjectVersion,
		raftpb.ForwardOpListObjectVersions,
		raftpb.ForwardOpHeadObjectVersion,
	} {
		spec, ok := lookupBucketForwardOpSpec(op)
		require.True(t, ok, "missing spec for %s", op)
		require.Equal(t, forwardFrameOnly, spec.transport, "wrong transport for %s", op)
	}
}

func TestBucketForwardOpSpecsInstallReceiverHandlers(t *testing.T) {
	for op, spec := range bucketForwardOpSpecs {
		switch spec.transport {
		case forwardFrameOnly:
			require.NotNil(t, spec.handleFrame, "frame-only op %s must install frame handler", op)
			require.Nil(t, spec.handleBody, "frame-only op %s must not install body handler", op)
			require.Nil(t, spec.handleRead, "frame-only op %s must not install read handler", op)
		case forwardBodyStream:
			require.NotNil(t, spec.handleBody, "body-stream op %s must install body handler", op)
			require.Nil(t, spec.handleRead, "body-stream op %s must not install read handler", op)
			if op != raftpb.ForwardOpAppendObject {
				require.NotNil(t, spec.handleFrame, "body-stream op %s must keep legacy frame handler", op)
			}
		case forwardReadStream:
			require.NotNil(t, spec.handleFrame, "read-stream op %s must keep legacy frame handler", op)
			require.Nil(t, spec.handleBody, "read-stream op %s must not install body handler", op)
			require.NotNil(t, spec.handleRead, "read-stream op %s must install read handler", op)
		default:
			t.Fatalf("unknown transport kind %d for %s", spec.transport, op)
		}
	}
}

func TestForwardReceiverRejectsStreamKindMismatchBeforeGroupLookup(t *testing.T) {
	rcv, _ := setupReceiver(t, "self")

	framePayload := encodeForwardPayload("missing", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))
	body := bytes.NewBufferString("stream body")
	bodyReply := rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: framePayload}, body)
	requireForwardStatus(t, bodyReply, raftpb.ForwardStatusInternal)
	require.Zero(t, body.Len())

	readReply, readBody := rcv.HandleRead(&transport.Message{Type: transport.StreamGroupForwardRead, Payload: framePayload})
	requireForwardStatus(t, readReply, raftpb.ForwardStatusInternal)
	require.Nil(t, readBody)

	bodyPayload := encodeForwardPayload("missing", raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil))
	readReply, readBody = rcv.HandleRead(&transport.Message{Type: transport.StreamGroupForwardRead, Payload: bodyPayload})
	requireForwardStatus(t, readReply, raftpb.ForwardStatusInternal)
	require.Nil(t, readBody)

	readPayload := encodeForwardPayload("missing", raftpb.ForwardOpGetObject, buildGetObjectArgs("b", "k"))
	body = bytes.NewBufferString("stream body")
	bodyReply = rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: readPayload}, body)
	requireForwardStatus(t, bodyReply, raftpb.ForwardStatusInternal)
	require.Zero(t, body.Len())
}

func TestForwardReceiverAllowedStreamKindReachesGroupLookup(t *testing.T) {
	rcv, _ := setupReceiver(t, "self")

	framePayload := encodeForwardPayload("missing", raftpb.ForwardOpHeadObject, buildHeadObjectArgs("b", "k"))
	requireForwardStatus(t, rcv.Handle(&transport.Message{Type: transport.StreamProposeGroupForward, Payload: framePayload}), raftpb.ForwardStatusNotVoter)

	bodyPayload := encodeForwardPayload("missing", raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil))
	bodyReply := rcv.HandleBody(&transport.Message{Type: transport.StreamGroupForwardBody, Payload: bodyPayload}, bytes.NewBufferString("stream body"))
	requireForwardStatus(t, bodyReply, raftpb.ForwardStatusNotVoter)

	readPayload := encodeForwardPayload("missing", raftpb.ForwardOpGetObject, buildGetObjectArgs("b", "k"))
	readReply, readBody := rcv.HandleRead(&transport.Message{Type: transport.StreamGroupForwardRead, Payload: readPayload})
	requireForwardStatus(t, readReply, raftpb.ForwardStatusNotVoter)
	require.Nil(t, readBody)
}

func requireForwardStatus(t *testing.T, msg *transport.Message, want raftpb.ForwardStatus) {
	t.Helper()
	require.NotNil(t, msg)
	fr := raftpb.GetRootAsForwardReply(msg.Payload, 0)
	require.Equal(t, want, fr.Status())
}
