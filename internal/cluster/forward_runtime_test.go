package cluster

import (
	"bytes"
	"context"
	"io"
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
