package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

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
