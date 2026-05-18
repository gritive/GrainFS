package cluster

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/transport"
)

func setupReadAtBenchReceiver() (*ForwardReceiver, *DataGroupManager) {
	mgr := NewDataGroupManager()
	return NewForwardReceiver(mgr), mgr
}

func setupReadAtBenchCoordinator(bucket, groupID string, peers []string) (*ClusterCoordinator, *recordingDialer) {
	base := &fakeBackend{}
	mgr := NewDataGroupManager()
	mgr.Add(NewDataGroup(groupID, peers))
	router := NewRouter(mgr)
	router.AssignBucket(bucket, groupID)
	meta := &fakeShardGroupSource{groups: map[string]ShardGroupEntry{
		groupID: {ID: groupID, PeerIDs: peers},
	}}
	d := &recordingDialer{
		replyByOp:     map[raftpb.ForwardOp][]byte{},
		streamReplyBy: map[raftpb.ForwardOp][]byte{},
		readReplyBy:   map[raftpb.ForwardOp][]byte{},
		readBodyBy:    map[raftpb.ForwardOp][]byte{},
	}
	sender := NewForwardSender(d.dial)
	c := NewClusterCoordinator(base, mgr, router, meta, "self").WithForwardSender(sender)
	return c, d
}

func BenchmarkForwardReadAtArgsPayload_4KiB(b *testing.B) {
	for b.Loop() {
		args := buildReadAtArgs("bench-bucket", "prefix/object", 128, 4*1024)
		payload := encodeForwardPayload("group-1", raftpb.ForwardOpReadAt, args)
		if len(payload) == 0 {
			b.Fatal("empty payload")
		}
	}
}

func BenchmarkForwardReadAtReplyParseCopy_4KiB(b *testing.B) {
	body := bytes.Repeat([]byte("r"), 4*1024)
	reply := buildReadAtReply(body)
	dst := make([]byte, len(body))

	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for b.Loop() {
		n, err := readAtReplyInto(reply, dst)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(body) {
			b.Fatalf("n = %d, want %d", n, len(body))
		}
	}
}

func BenchmarkForwardReceiverHandleReadAtSmall_4KiB(b *testing.B) {
	rcv, mgr := setupReadAtBenchReceiver()
	gb := newTestGroupBackend(b, "g1")
	mgr.Add(NewDataGroupWithBackend("g1", []string{"test-node"}, gb))

	body := bytes.Repeat([]byte("b"), 8*1024)
	if _, err := gb.PutObject(context.Background(), "bk", "small", bytes.NewReader(body), "application/octet-stream"); err != nil {
		b.Fatal(err)
	}
	payload := encodeForwardPayload("g1", raftpb.ForwardOpReadAt, buildReadAtArgs("bk", "small", 128, 4*1024))
	req := &transport.Message{Type: transport.StreamProposeGroupForward, Payload: payload}

	b.SetBytes(4 * 1024)
	b.ResetTimer()
	for b.Loop() {
		reply := rcv.Handle(req)
		if reply == nil {
			b.Fatal("nil reply")
		}
		if err := parseReplyStatus(reply.Payload); err != nil {
			b.Fatal(err)
		}
		fr := raftpb.GetRootAsForwardReply(reply.Payload, 0)
		if fr.ReadBodyLength() != 4*1024 {
			b.Fatalf("read body length %d, want %d", fr.ReadBodyLength(), 4*1024)
		}
	}
}

func BenchmarkClusterCoordinatorReadAtForwardSmall_4KiB(b *testing.B) {
	c, d := setupReadAtBenchCoordinator("bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("c"), 4*1024)
	d.replyByOp[raftpb.ForwardOpReadAt] = buildReadAtReply(body)
	c.forward.WithReadStreamDialer(d.readStream)
	dst := make([]byte, len(body))

	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for b.Loop() {
		n, err := c.ReadAt(context.Background(), "bk", "small", 128, dst)
		if err != nil {
			b.Fatal(err)
		}
		if n != len(body) {
			b.Fatalf("read %d bytes, want %d", n, len(body))
		}
		if !bytes.Equal(dst[:n], body) {
			b.Fatal("body mismatch")
		}
	}
}

func BenchmarkClusterCoordinatorReadAtForwardStreamCutoff_5MiBPlus1(b *testing.B) {
	c, d := setupReadAtBenchCoordinator("bk", "g1", []string{"a"})
	body := bytes.Repeat([]byte("s"), DefaultMaxForwardBodyBytes+1)
	d.readReplyBy[raftpb.ForwardOpReadAt] = buildOKReply()
	d.readBodyBy[raftpb.ForwardOpReadAt] = body
	c.forward.WithReadStreamDialer(d.readStream)
	dst := make([]byte, len(body))

	b.SetBytes(int64(len(body)))
	b.ResetTimer()
	for b.Loop() {
		n, err := c.ReadAt(context.Background(), "bk", "large", 0, dst)
		if err != nil && err != io.EOF {
			b.Fatal(err)
		}
		if n != len(body) {
			b.Fatalf("read %d bytes, want %d", n, len(body))
		}
		if !bytes.Equal(dst[:n], body) {
			b.Fatal("body mismatch")
		}
	}
}
