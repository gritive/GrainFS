package cluster

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestForwardRuntimeReadObjectUsesReadStream(t *testing.T) {
	d := &recordingDialer{
		readReplyBy: map[raftpb.ForwardOp][]byte{},
		readBodyBy:  map[raftpb.ForwardOp][]byte{},
	}
	body := bytes.Repeat([]byte("r"), 128*1024)
	d.readReplyBy[raftpb.ForwardOpGetObject] = buildGetObjectReply(
		&storage.Object{Key: "k", Size: int64(len(body)), ETag: "etag", ContentType: "application/octet-stream"},
		"bk", nil,
	)
	d.readBodyBy[raftpb.ForwardOpGetObject] = body

	rt := forwardRuntime{sender: NewForwardSender(d.dial).WithReadStreamDialer(d.readStream)}
	rc, obj, err := rt.readObject(
		context.Background(),
		RouteTarget{GroupID: "g1", Peers: []string{"peer-a"}},
		raftpb.ForwardOpGetObject,
		buildGetObjectArgs("bk", "k"),
	)
	require.NoError(t, err)
	defer rc.Close()
	got, err := io.ReadAll(rc)
	require.NoError(t, err)
	require.Equal(t, int64(len(body)), obj.Size)
	require.Equal(t, body, got)
	require.Empty(t, d.calls)
	require.Len(t, d.readCalls, 1)
	require.Equal(t, raftpb.ForwardOpGetObject, d.readCalls[0].op)
	require.Equal(t, "g1", d.readCalls[0].gid)
}

func TestForwardRuntimeMutateFrameSendsStatusOnlyMutation(t *testing.T) {
	d := &recordingDialer{replyByOp: map[raftpb.ForwardOp][]byte{}}
	d.replyByOp[raftpb.ForwardOpSetObjectACL] = buildOKReply()

	rt := forwardRuntime{sender: NewForwardSender(d.dial)}
	err := rt.mutateFrame(
		context.Background(),
		RouteTarget{GroupID: "g1", Peers: []string{"peer-a"}},
		raftpb.ForwardOpSetObjectACL,
		buildSetObjectACLArgs("bk", "k", 7),
	)
	require.NoError(t, err)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpSetObjectACL, d.calls[0].op)
	require.Equal(t, "g1", d.calls[0].gid)
}

func TestForwardRuntimePutObjectFrameRejectsSizeMismatch(t *testing.T) {
	d := &recordingDialer{replyByOp: map[raftpb.ForwardOp][]byte{}}
	d.replyByOp[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: 99, ETag: "etag", ContentType: "text/plain"},
		"bk",
	)

	rt := forwardRuntime{sender: NewForwardSender(d.dial), maxBody: DefaultMaxForwardBodyBytes}
	_, err := rt.putObject(
		context.Background(),
		RouteTarget{GroupID: "g1", Peers: []string{"peer-a"}},
		ShardGroupEntry{ID: "g1"},
		storage.PutObjectRequest{
			Bucket:      "bk",
			Key:         "k",
			Body:        bytes.NewReader([]byte("small")),
			ContentType: "text/plain",
		},
		time.Now(),
	)
	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
}

func TestForwardRuntimeHeadObjectDecodesFrameReply(t *testing.T) {
	d := &recordingDialer{replyByOp: map[raftpb.ForwardOp][]byte{}}
	d.replyByOp[raftpb.ForwardOpHeadObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: 7, ETag: "etag", ContentType: "text/plain"},
		"bk",
	)

	rt := forwardRuntime{sender: NewForwardSender(d.dial)}
	obj, err := rt.headObject(
		context.Background(),
		RouteTarget{GroupID: "g1", Peers: []string{"peer-a"}},
		raftpb.ForwardOpHeadObject,
		buildHeadObjectArgs("bk", "k"),
		"bk",
		"k",
	)
	require.NoError(t, err)
	require.Equal(t, int64(7), obj.Size)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpHeadObject, d.calls[0].op)
}

func TestForwardRuntimeCreateMultipartUploadDecodesFrameReply(t *testing.T) {
	d := &recordingDialer{replyByOp: map[raftpb.ForwardOp][]byte{}}
	d.replyByOp[raftpb.ForwardOpCreateMultipartUpload] = buildUploadReply("bk", "k", "upload-1")

	rt := forwardRuntime{sender: NewForwardSender(d.dial)}
	upload, err := rt.createMultipartUpload(
		context.Background(),
		RouteTarget{GroupID: "g1", Peers: []string{"peer-a"}},
		"bk",
		"k",
		"text/plain",
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, "upload-1", upload.UploadID)
	require.Len(t, d.calls, 1)
	require.Equal(t, raftpb.ForwardOpCreateMultipartUpload, d.calls[0].op)
}

func TestForwardRuntimeAppendObjectStreamsBody(t *testing.T) {
	d := &recordingDialer{streamReplyBy: map[raftpb.ForwardOp][]byte{}}
	d.streamReplyBy[raftpb.ForwardOpAppendObject] = buildObjectReply(&storage.Object{
		Key:       "k",
		Size:      8,
		VersionID: "v1",
		ETag:      "etag-v1",
	}, "bk")

	rt := forwardRuntime{sender: NewForwardSender(d.dial).WithStreamDialer(d.stream), maxBody: DefaultMaxForwardBodyBytes}
	obj, err := rt.appendObject(
		context.Background(),
		RouteTarget{GroupID: "g1", Peers: []string{"peer-a"}},
		ShardGroupEntry{ID: "g1"},
		"bk",
		"k",
		5,
		strings.NewReader("abc"),
	)
	require.NoError(t, err)
	require.Equal(t, int64(8), obj.Size)
	require.Len(t, d.streamCalls, 1)
	require.Equal(t, raftpb.ForwardOpAppendObject, d.streamCalls[0].op)
	require.Equal(t, []byte("abc"), d.streamCalls[0].rawly)
}
