package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func headObjectArgsBytes(t *testing.T, bucket, key string) []byte {
	t.Helper()
	b := flatbuffers.NewBuilder(64)
	bk := b.CreateString(bucket)
	k := b.CreateString(key)
	raftpb.HeadObjectArgsStart(b)
	raftpb.HeadObjectArgsAddBucket(b, bk)
	raftpb.HeadObjectArgsAddKey(b, k)
	off := raftpb.HeadObjectArgsEnd(b)
	b.Finish(off)
	return b.FinishedBytes()
}

func okReplyBytes(t *testing.T) []byte {
	t.Helper()
	return buildSimpleReply(raftpb.ForwardStatusOK, "")
}

func notLeaderReplyBytes(t *testing.T, hint string) []byte {
	t.Helper()
	return buildSimpleReply(raftpb.ForwardStatusNotLeader, hint)
}

// TestForwardPayload_HeaderRoundtrip verifies encode/decode roundtrip — the
// payload format is the contract between sender and receiver. A regression
// here breaks every routing op.
func TestForwardPayload_HeaderRoundtrip(t *testing.T) {
	args := headObjectArgsBytes(t, "bucket-A", "key-1")
	payload := encodeForwardPayload("group-1", raftpb.ForwardOpHeadObject, args)

	gid, op, decoded, err := decodeForwardPayload(payload)
	require.NoError(t, err)
	require.Equal(t, "group-1", gid)
	require.Equal(t, raftpb.ForwardOpHeadObject, op)
	require.Equal(t, args, decoded)
}

func TestForwardPayload_TruncatedHeader(t *testing.T) {
	_, _, _, err := decodeForwardPayload([]byte{0x00, 0x01})
	require.ErrorIs(t, err, ErrShortHeader)
}

func TestForwardPayload_TruncatedArgs(t *testing.T) {
	// Header claims 100-byte args but only 5 follow.
	payload := []byte{
		0, 0, 0, 2, // groupIDLen=2
		'g', '1',
		byte(raftpb.ForwardOpHeadObject),
		0, 0, 0, 100, // argsLen=100
		1, 2, 3, 4, 5, // only 5 bytes
	}
	_, _, _, err := decodeForwardPayload(payload)
	require.ErrorIs(t, err, ErrTruncatedArgs)
}

// TestForwardSender_TryEachPeer_FirstDownNextSucceeds verifies the recovery
// path when the first peer in the candidate list is unreachable. Without
// this, a single down node renders the entire group unreachable to non-voters
// (Codex Gap 3 — production landmine).
func TestForwardSender_TryEachPeer_FirstDownNextSucceeds(t *testing.T) {
	var connected []string
	dialer := func(peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if peer == "down-peer" {
			return nil, errors.New("connection refused")
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)
	reply, err := s.Send(context.Background(),
		[]string{"down-peer", "healthy-peer"}, "group-1",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"down-peer", "healthy-peer"}, connected)
}

// TestForwardSender_NotLeaderRedirect_OnceOnly verifies the cold-path leader
// discovery. NotLeader hint is the contract that lets us avoid maintaining a
// leader cache (PR-G+H plan-eng-review issue 1A).
func TestForwardSender_NotLeaderRedirect_OnceOnly(t *testing.T) {
	var connected []string
	dialer := func(peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if peer == "peer-A" {
			return notLeaderReplyBytes(t, "peer-B"), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)
	reply, err := s.Send(context.Background(),
		[]string{"peer-A", "peer-C"}, "group-1",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"peer-A", "peer-B"}, connected)
	// Verify final reply is OK (came from peer-B, not the NotLeader from peer-A)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusOK, fr.Status())
}

// TestForwardSender_AllPeersDown_ReturnsErrNoReachable verifies that the
// caller can distinguish "no peer responded" from "peer responded with error".
// S3 client retry policy depends on this distinction.
func TestForwardSender_AllPeersDown_ReturnsErrNoReachable(t *testing.T) {
	dialer := func(peer string, payload []byte) ([]byte, error) {
		return nil, errors.New("connection refused")
	}
	s := NewForwardSender(dialer)
	_, err := s.Send(context.Background(),
		[]string{"a", "b"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.ErrorIs(t, err, ErrNoReachablePeer)
}

// TestForwardSender_NotLeaderHintFails_FallthroughOriginalReply verifies that
// when the hint dial also fails, we don't loop forever — we return the original
// NotLeader reply and let the caller retry from a fresh node.
func TestForwardSender_NotLeaderHintFails_FallthroughOriginalReply(t *testing.T) {
	dialer := func(peer string, payload []byte) ([]byte, error) {
		if peer == "peer-A" {
			return notLeaderReplyBytes(t, "peer-DEAD"), nil
		}
		if peer == "peer-DEAD" {
			return nil, errors.New("hint dial failed")
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)
	reply, err := s.Send(context.Background(),
		[]string{"peer-A"}, "group-1",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.NoError(t, err)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	// Caller sees NotLeader status — they will retry from a fresh node.
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status())
}

// TestForwardSender_ContextCanceled_StopsImmediately verifies caller context
// cancellation is honored — long-running S3 client cancellations must not
// block on subsequent peer dials.
func TestForwardSender_ContextCanceled_StopsImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	dialer := func(peer string, payload []byte) ([]byte, error) {
		t.Fatal("dialer must not be called when ctx already canceled")
		return nil, nil
	}
	s := NewForwardSender(dialer)
	_, err := s.Send(ctx, []string{"a"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.ErrorIs(t, err, context.Canceled)
}

func TestForwardSender_ResolveLeaderPeers_UsesNotLeaderHint(t *testing.T) {
	var connected []string
	dialer := func(peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if peer == "peer-A" {
			return notLeaderReplyBytes(t, "peer-B"), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)
	peers := s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-C"}, "group-1", "b", "k")
	require.Equal(t, []string{"peer-B", "peer-A", "peer-C"}, peers)
	require.Equal(t, []string{"peer-A"}, connected)
}

func TestForwardSender_ResolveLeaderPeers_TreatsNoSuchKeyAsLeader(t *testing.T) {
	dialer := func(peer string, payload []byte) ([]byte, error) {
		return buildSimpleReply(raftpb.ForwardStatusNoSuchKey, ""), nil
	}
	s := NewForwardSender(dialer)
	peers := s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-B"}, "group-1", "missing-b", "missing-k")
	require.Equal(t, []string{"peer-A", "peer-B"}, peers)
}

func TestForwardSender_SendStream_NotLeaderWithoutRewindDoesNotRetryBody(t *testing.T) {
	calls := 0
	streamDialer := func(peer string, payload []byte, body io.Reader) ([]byte, error) {
		calls++
		_, _ = io.Copy(io.Discard, body)
		return notLeaderReplyBytes(t, "peer-B"), nil
	}
	s := NewForwardSender(func(string, []byte) ([]byte, error) {
		return okReplyBytes(t), nil
	}).WithStreamDialer(streamDialer)

	body := io.LimitReader(bytes.NewReader([]byte("payload")), int64(len("payload")))
	reply, err := s.SendStream(context.Background(), []string{"peer-A"}, "g",
		raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil), body)
	require.NoError(t, err)
	require.Equal(t, 1, calls)
	fr := raftpb.GetRootAsForwardReply(reply, 0)
	require.Equal(t, raftpb.ForwardStatusNotLeader, fr.Status())
}

func TestForwardSender_SendStream_BackpressureLimit(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	streamDialer := func(peer string, payload []byte, body io.Reader) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release
		_, _ = io.Copy(io.Discard, body)
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(func(string, []byte) ([]byte, error) {
		t.Fatal("single-message dialer must not be used for streamed body")
		return nil, nil
	}).WithStreamDialer(streamDialer).WithMaxForwardStreams(1)

	done := make(chan error, 1)
	go func() {
		_, err := s.SendStream(context.Background(), []string{"peer-a"}, "g",
			raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil), bytes.NewReader([]byte("first")))
		done <- err
	}()
	<-started

	_, err := s.SendStream(context.Background(), []string{"peer-a"}, "g",
		raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k2", "text/plain", nil), bytes.NewReader([]byte("second")))
	require.ErrorIs(t, err, storage.ErrForwardBackpressure)

	close(release)
	require.NoError(t, <-done)
}
