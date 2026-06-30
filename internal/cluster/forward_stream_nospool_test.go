package cluster

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// T-A2: a STREAMED forwarded PUT must thread the sender's known decoded length
// through the PutObjectArgs frame so the receiver builds a sized request and
// takes the no-spool streaming path.

func TestForwardStreamDecodedLength(t *testing.T) {
	exact := int64(4096)
	advisory := int64(4096)
	negative := int64(-7)
	tests := []struct {
		name string
		req  storage.PutObjectRequest
		want int64
	}{
		{"exact hint -> length", storage.PutObjectRequest{SizeHint: &exact, SizeHintExact: true}, 4096},
		{"non-exact hint -> unknown", storage.PutObjectRequest{SizeHint: &advisory, SizeHintExact: false}, -1},
		{"nil hint -> unknown", storage.PutObjectRequest{}, -1},
		{"negative exact -> unknown", storage.PutObjectRequest{SizeHint: &negative, SizeHintExact: true}, -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, forwardStreamDecodedLength(tc.req))
		})
	}
}

func TestBuildForwardStreamPutRequest_SizedWhenDecodedLengthPresent(t *testing.T) {
	args := buildPutObjectArgsWithSSE("b", "k", "text/plain", nil, "", nil, "", 0, versioningStateUnknown, 12345)
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)

	req := buildForwardStreamPutRequest(pa, bytes.NewReader(nil))

	require.NotNil(t, req.SizeHint, "sized frame must carry SizeHint so the backend takes the no-spool path")
	require.Equal(t, int64(12345), *req.SizeHint)
	require.True(t, req.SizeHintExact)
}

func TestBuildForwardStreamPutRequest_UnsizedWhenAbsent(t *testing.T) {
	// Old sender: decoded_length absent -> default -1 -> unsized request (spool).
	args := buildPutObjectArgs("b", "k", "text/plain", nil)
	pa := raftpb.GetRootAsPutObjectArgs(args, 0)
	require.Equal(t, int64(-1), pa.DecodedLength(), "old-sender frame reads decoded_length as -1")

	req := buildForwardStreamPutRequest(pa, bytes.NewReader(nil))

	require.Nil(t, req.SizeHint, "unsized frame must leave SizeHint nil so the spool fallback applies")
	require.False(t, req.SizeHintExact)
}

// newChunkCapableStreamGroupBackend wires a shardGroup + small chunk threshold
// onto the test GroupBackend so the no-spool streaming gate
// (req.SizeHint != nil && req.SizeHintExact && b.shardGroup != nil) can fire.
func newChunkCapableStreamGroupBackend(t *testing.T, groupID string) *GroupBackend {
	t.Helper()
	gb := newTestGroupBackend(t, groupID)
	gb.SetShardGroupSource(&fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		groupID: {ID: groupID, PeerIDs: []string{"test-node"}},
	}})
	gb.chunkedPutChunkSize = 1 << 10 // 1 KiB so a small streamed body still chunks
	return gb
}

// TestForwardReceiver_HandlePutObjectStream_SizedTakesNoSpoolPath is the
// RED->GREEN discriminator. With a shardGroup wired, the no-spool path enforces
// the exact size via exactObjectSizeReader: a body SHORTER than the stamped
// decoded_length must fail (ErrUnexpectedEOF -> non-OK). The spool fallback
// (pre-fix, decoded_length ignored) would store the short body and reply OK, so
// this asymmetry proves the sized streaming path is actually taken.
func TestForwardReceiver_HandlePutObjectStream_SizedTakesNoSpoolPath(t *testing.T) {
	gb := newChunkCapableStreamGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	body := []byte("streamed-body-content")

	// Positive: exact decoded_length -> OK and round-trips byte-identical.
	okArgs := buildPutObjectArgsWithSSE("bucket", "sized-ok", "text/plain", nil, "", nil, "", 0, versioningStateUnknown, int64(len(body)))
	okPayload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, okArgs)
	okReply, _ := rcv.HandleBody(okPayload, bytes.NewReader(body))
	require.Equal(t, raftpb.ForwardStatusOK, raftpb.GetRootAsForwardReply(okReply, 0).Status())

	rc, _, err := gb.GetObject(context.Background(), "bucket", "sized-ok")
	require.NoError(t, err)
	defer rc.Close()
	got := bytes.NewBuffer(nil)
	_, err = got.ReadFrom(rc)
	require.NoError(t, err)
	require.Equal(t, body, got.Bytes(), "sized forwarded stream must round-trip byte-identical")

	// Negative discriminator: decoded_length larger than the body -> the no-spool
	// exactObjectSizeReader rejects the short read. Only the streaming path does
	// this; the spool fallback would accept it.
	overArgs := buildPutObjectArgsWithSSE("bucket", "sized-short", "text/plain", nil, "", nil, "", 0, versioningStateUnknown, int64(len(body)+5))
	overPayload := encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, overArgs)
	overReply, _ := rcv.HandleBody(overPayload, bytes.NewReader(body))
	require.NotEqual(t, raftpb.ForwardStatusOK, raftpb.GetRootAsForwardReply(overReply, 0).Status(),
		"a body shorter than the stamped decoded_length must fail on the no-spool streaming path")
}

// TestClusterCoordinator_PutObject_StreamForward_StampsDecodedLength closes the
// sender->wire loop: an exact SizeHint on a streamed forwarded PUT must land in
// the PutObjectArgs frame as decoded_length so the receiver can size the request.
// The convenience PutObject wrapper carries no SizeHint and must stamp -1.
func TestClusterCoordinator_PutObject_StreamForward_StampsDecodedLength(t *testing.T) {
	c, d := setupCoordWithForward(t, "bk", "g1", []string{"a"})
	c.forward.WithStreamDialer(d.stream)
	c.maxBody = 128 * 1024
	body := bytes.Repeat([]byte("z"), int(c.maxBody)+1024)
	size := int64(len(body))
	d.streamReplyBy[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k", Size: size, ETag: "etag-stream"}, "bk",
	)

	_, err := c.PutObjectWithRequest(context.Background(), storage.PutObjectRequest{
		Bucket: "bk", Key: "k", Body: bytes.NewReader(body),
		ContentType: "application/octet-stream", SizeHint: &size, SizeHintExact: true,
	})
	require.NoError(t, err)
	require.Len(t, d.streamCalls, 1)
	args := raftpb.GetRootAsPutObjectArgs(d.streamCalls[0].args, 0)
	require.Equal(t, size, args.DecodedLength(),
		"sender stream branch must stamp req.SizeHint as decoded_length")

	// Convenience PutObject (no SizeHint) keeps the old-sender default -1.
	d.streamCalls = nil
	d.streamReplyBy[raftpb.ForwardOpPutObject] = buildObjectReply(
		&storage.Object{Key: "k2", Size: size, ETag: "etag-stream"}, "bk",
	)
	_, err = c.PutObject(context.Background(), "bk", "k2", bytes.NewReader(body), "application/octet-stream")
	require.NoError(t, err)
	require.Len(t, d.streamCalls, 1)
	noHint := raftpb.GetRootAsPutObjectArgs(d.streamCalls[0].args, 0)
	require.Equal(t, int64(-1), noHint.DecodedLength(),
		"unsized convenience PUT must leave decoded_length at the -1 wire default")
}

// TestForwardReceiver_HandlePutObjectStream_SizedContentMD5 guards the
// newly-reachable combination: a sized stream with Content-MD5. Before T-A2 a
// forwarded stream always spooled (validating MD5 on the spool); now it takes
// the no-spool tee+beforeCommit validation. Both good and bad digests must
// behave identically to the spool path.
func TestForwardReceiver_HandlePutObjectStream_SizedContentMD5(t *testing.T) {
	gb := newChunkCapableStreamGroupBackend(t, "group-1")
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroupWithBackend("group-1", []string{"test-node"}, gb))
	rcv := NewForwardReceiver(mgr)

	body := []byte("streamed-body-content")
	sum := md5.Sum(body)
	goodMD5 := hex.EncodeToString(sum[:])

	goodArgs := buildPutObjectArgsWithSSE("bucket", "md5-ok", "text/plain", nil, "", nil, goodMD5, 0, versioningStateUnknown, int64(len(body)))
	goodReply, _ := rcv.HandleBody(encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, goodArgs), bytes.NewReader(body))
	require.Equal(t, raftpb.ForwardStatusOK, raftpb.GetRootAsForwardReply(goodReply, 0).Status(),
		"sized stream with a matching Content-MD5 must commit")

	badArgs := buildPutObjectArgsWithSSE("bucket", "md5-bad", "text/plain", nil, "", nil, "deadbeefdeadbeefdeadbeefdeadbeef", 0, versioningStateUnknown, int64(len(body)))
	badReply, _ := rcv.HandleBody(encodeForwardPayload("group-1", raftpb.ForwardOpPutObject, badArgs), bytes.NewReader(body))
	require.Equal(t, raftpb.ForwardStatusBadDigest, raftpb.GetRootAsForwardReply(badReply, 0).Status(),
		"sized stream with a mismatched Content-MD5 must fail with BadDigest on the no-spool path")
}
