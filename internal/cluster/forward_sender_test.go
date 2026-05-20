package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

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

func TestParseReplyStatus_InsufficientPlacementTargets(t *testing.T) {
	err := parseReplyStatus(buildSimpleReply(raftpb.ForwardStatusInsufficientPlacementTargets, ""))
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPlacementTargetsUnavailable)
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

func TestBuildPutObjectArgs_SmallBodyAllocationBound(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 64*1024)
	const runs = 200

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		args := buildPutObjectArgs("warp-grainfs-postcommits-20260516", "prefix/object.rnd", "application/octet-stream", body)
		require.Greater(t, len(args), len(body))
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	avgAlloc := float64(after.TotalAlloc-before.TotalAlloc) / runs
	require.Lessf(t, avgAlloc, float64(len(body))*1.5, "avg allocation per buildPutObjectArgs call = %.0f bytes", avgAlloc)
}

func BenchmarkBuildPutObjectArgs_64KiB(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 64*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := buildPutObjectArgs("warp-grainfs-postcommits-20260516", "prefix/object.rnd", "application/octet-stream", body)
		if len(args) <= len(body) {
			b.Fatalf("args length %d <= body length %d", len(args), len(body))
		}
	}
}

func TestBuildUploadPartArgs_5MiBAllocationBound(t *testing.T) {
	body := bytes.Repeat([]byte("x"), 5*1024*1024)
	const runs = 20

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	for i := 0; i < runs; i++ {
		args := buildUploadPartArgs("bucket", "multipart/object.rnd", "upload-id", int32(i+1), body)
		require.Greater(t, len(args), len(body))
	}
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	avgAlloc := float64(after.TotalAlloc-before.TotalAlloc) / runs
	require.Lessf(t, avgAlloc, float64(len(body))*1.5, "avg allocation per buildUploadPartArgs call = %.0f bytes", avgAlloc)
}

func BenchmarkBuildUploadPartArgs_5MiB(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 5*1024*1024)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		args := buildUploadPartArgs("bucket", "multipart/object.rnd", "upload-id", int32(i+1), body)
		if len(args) <= len(body) {
			b.Fatalf("args length %d <= body length %d", len(args), len(body))
		}
	}
}

// TestForwardSender_TryEachPeer_FirstDownNextSucceeds verifies the recovery
// path when the first peer in the candidate list is unreachable. Without
// this, a single down node renders the entire group unreachable to non-voters
// (Codex Gap 3 — production landmine).
func TestForwardSender_TryEachPeer_FirstDownNextSucceeds(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
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
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
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

func TestForwardSender_NotLeaderMalformedHintDoesNotPanic(t *testing.T) {
	replyWithBadHint := notLeaderReplyBytes(t, "peer-B")
	replyWithBadHint[10] = 0x01
	require.True(t, isNotLeaderReply(replyWithBadHint))
	require.Empty(t, extractLeaderHint(replyWithBadHint))

	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		return replyWithBadHint, nil
	}
	s := NewForwardSender(dialer)
	reply, err := s.Send(context.Background(),
		[]string{"peer-A"}, "group-1",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.NoError(t, err)
	require.Equal(t, replyWithBadHint, reply)
	require.Equal(t, []string{"peer-A"}, connected)
}

// TestForwardSender_AllPeersDown_ReturnsErrNoReachable verifies that the
// caller can distinguish "no peer responded" from "peer responded with error".
// S3 client retry policy depends on this distinction.
func TestForwardSender_AllPeersDown_ReturnsErrNoReachable(t *testing.T) {
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		return nil, errors.New("connection refused")
	}
	s := NewForwardSender(dialer)
	_, err := s.Send(context.Background(),
		[]string{"a", "b"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.ErrorIs(t, err, ErrNoReachablePeer)
}

func TestForwardSender_RetriesPeerSweepWithinCallerDeadline(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if len(connected) < 3 {
			return nil, errors.New("connection refused")
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	reply, err := s.Send(ctx, []string{"a", "b"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))

	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"a", "b", "a"}, connected)
}

func TestForwardSender_RetriesNotLeaderWithoutHintWithinCallerDeadline(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if len(connected) < 3 {
			return notLeaderReplyBytes(t, ""), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	reply, err := s.Send(ctx, []string{"a", "b"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))

	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"a", "b", "a"}, connected)
}

func TestForwardSender_ReadinessRetryAddsDeadlineForBackgroundCaller(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if len(connected) < 3 {
			return notLeaderReplyBytes(t, ""), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer).WithReadinessRetry(500 * time.Millisecond)

	reply, err := s.Send(context.Background(), []string{"a", "b"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))

	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"a", "b", "a"}, connected)
}

// TestForwardSender_NotLeaderHintFails_FallthroughOriginalReply verifies that
// when the hint dial also fails, we don't loop forever — we return the original
// NotLeader reply and let the caller retry from a fresh node.
func TestForwardSender_NotLeaderHintFails_FallthroughOriginalReply(t *testing.T) {
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
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
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		t.Fatal("dialer must not be called when ctx already canceled")
		return nil, nil
	}
	s := NewForwardSender(dialer)
	_, err := s.Send(ctx, []string{"a"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))
	require.ErrorIs(t, err, context.Canceled)
}

func TestForwardSender_SendUsesPerCallTimeoutContext(t *testing.T) {
	started := make(chan struct{})
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s := NewForwardSender(dialer)
	s.timeout = 20 * time.Millisecond

	start := time.Now()
	_, err := s.Send(context.Background(), []string{"slow-peer"}, "g",
		raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "b", "k"))

	require.ErrorIs(t, err, ErrNoReachablePeer)
	require.Less(t, time.Since(start), 200*time.Millisecond)
	<-started
}

func TestForwardSender_ResolveLeaderPeers_UsesNotLeaderHint(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if peer == "peer-A" {
			return notLeaderReplyBytes(t, "peer-B"), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)
	peers := s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-C"}, "group-1", "b", "k")
	require.Equal(t, []string{"peer-B", "peer-A", "peer-C"}, peers)
	require.Equal(t, []string{"peer-A", "peer-B"}, connected)
}

func TestForwardSender_ResolveLeaderPeers_SkipsUnreachableHint(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		switch peer {
		case "peer-A":
			return notLeaderReplyBytes(t, "node-id-only"), nil
		case "node-id-only":
			return nil, errors.New("not a dialable address")
		case "peer-C":
			return buildSimpleReply(raftpb.ForwardStatusNoSuchKey, ""), nil
		default:
			return nil, errors.New("unexpected peer")
		}
	}
	s := NewForwardSender(dialer)
	peers := s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-C"}, "group-1", "b", "k")
	require.Equal(t, []string{"peer-C", "peer-A"}, peers)
	require.Equal(t, []string{"peer-A", "node-id-only", "peer-C"}, connected)
}

func TestForwardSender_ResolveLeaderPeers_ResolvesNodeIDHint(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		switch peer {
		case "peer-A":
			return notLeaderReplyBytes(t, "node-C"), nil
		case "10.0.0.3:7000":
			return buildSimpleReply(raftpb.ForwardStatusNoSuchKey, ""), nil
		default:
			return nil, errors.New("unexpected peer")
		}
	}
	s := NewForwardSender(dialer).WithLeaderHintResolver(func(hint string) string {
		if hint == "node-C" {
			return "10.0.0.3:7000"
		}
		return hint
	})
	peers := s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-B"}, "group-1", "b", "k")
	require.Equal(t, []string{"10.0.0.3:7000", "peer-A", "peer-B"}, peers)
	require.Equal(t, []string{"peer-A", "10.0.0.3:7000"}, connected)
}

func TestForwardSender_ResolveLeaderPeers_TreatsNoSuchKeyAsLeader(t *testing.T) {
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		return buildSimpleReply(raftpb.ForwardStatusNoSuchKey, ""), nil
	}
	s := NewForwardSender(dialer)
	peers := s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-B"}, "group-1", "missing-b", "missing-k")
	require.Equal(t, []string{"peer-A", "peer-B"}, peers)
}

func TestForwardSender_ResolveLeaderPeers_UsesCachedLeader(t *testing.T) {
	var connected []string
	dialer := func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		connected = append(connected, peer)
		if peer == "peer-A" {
			return notLeaderReplyBytes(t, "peer-B"), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(dialer)

	peers := s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-C"}, "group-1", "b", "k1")
	require.Equal(t, []string{"peer-B", "peer-A", "peer-C"}, peers)
	require.Equal(t, []string{"peer-A", "peer-B"}, connected)

	peers = s.ResolveLeaderPeers(context.Background(), []string{"peer-A", "peer-C"}, "group-1", "b", "k2")
	require.Equal(t, []string{"peer-B", "peer-A", "peer-C"}, peers)
	require.Equal(t, []string{"peer-A", "peer-B"}, connected, "second resolve should use cached leader without probing")
}

func TestForwardSender_SendStream_NotLeaderWithoutRewindDoesNotRetryBody(t *testing.T) {
	calls := 0
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		calls++
		_, _ = io.Copy(io.Discard, body)
		return notLeaderReplyBytes(t, "peer-B"), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
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

func TestForwardSender_SendStreamRetriesPeerSweepWithinCallerDeadline(t *testing.T) {
	var connected []string
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		connected = append(connected, peer)
		if len(connected) < 3 {
			_, _ = io.Copy(io.Discard, body)
			return nil, errors.New("connection refused")
		}
		got, err := io.ReadAll(body)
		require.NoError(t, err)
		require.Equal(t, []byte("payload"), got)
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		t.Fatal("single-message dialer must not be used for streamed body")
		return nil, nil
	}).WithStreamDialer(streamDialer)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	reply, err := s.SendStream(ctx, []string{"a", "b"}, "g",
		raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil), bytes.NewReader([]byte("payload")))

	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"a", "b", "a"}, connected)
}

func TestForwardSender_SendStreamAppendDoesNotRetryAfterDialError(t *testing.T) {
	var connected []string
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		connected = append(connected, peer)
		_, _ = io.Copy(io.Discard, body)
		return nil, errors.New("response lost after append may have applied")
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		t.Fatal("single-message dialer must not be used for streamed body")
		return nil, nil
	}).WithStreamDialer(streamDialer)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	reply, err := s.SendStream(ctx, []string{"a", "b"}, "g",
		raftpb.ForwardOpAppendObject, buildAppendObjectForwardArgs("b", "k", 16), bytes.NewReader([]byte("payload")))

	require.Error(t, err)
	require.Nil(t, reply)
	require.Equal(t, []string{"a"}, connected)
}

func TestForwardSender_SendStreamRetriesNotLeaderWithoutHintWithinCallerDeadline(t *testing.T) {
	var connected []string
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		connected = append(connected, peer)
		_, _ = io.Copy(io.Discard, body)
		if len(connected) < 3 {
			return notLeaderReplyBytes(t, ""), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		t.Fatal("single-message dialer must not be used for streamed body")
		return nil, nil
	}).WithStreamDialer(streamDialer)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	reply, err := s.SendStream(ctx, []string{"a", "b"}, "g",
		raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil), bytes.NewReader([]byte("payload")))

	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"a", "b", "a"}, connected)
}

func TestForwardSender_SendStreamReadinessRetryAddsDeadlineForBackgroundCaller(t *testing.T) {
	var connected []string
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		connected = append(connected, peer)
		_, _ = io.Copy(io.Discard, body)
		if len(connected) < 3 {
			return notLeaderReplyBytes(t, ""), nil
		}
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		t.Fatal("single-message dialer must not be used for streamed body")
		return nil, nil
	}).WithStreamDialer(streamDialer).WithReadinessRetry(500 * time.Millisecond)

	reply, err := s.SendStream(context.Background(), []string{"a", "b"}, "g",
		raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil), bytes.NewReader([]byte("payload")))

	require.NoError(t, err)
	require.NotEmpty(t, reply)
	require.Equal(t, []string{"a", "b", "a"}, connected)
}

func TestForwardSender_SendStreamDefaultLimitHandlesWarpMultipartConcurrency(t *testing.T) {
	const concurrency = 32
	started := make(chan struct{}, concurrency)
	release := make(chan struct{})
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		started <- struct{}{}
		<-release
		_, _ = io.Copy(io.Discard, body)
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		t.Fatal("single-message dialer must not be used for streamed body")
		return nil, nil
	}).WithStreamDialer(streamDialer)
	defer close(release)

	done := make(chan error, concurrency)
	for i := 0; i < concurrency; i++ {
		i := i
		go func() {
			_, err := s.SendStream(context.Background(), []string{"peer-a"}, "g",
				raftpb.ForwardOpUploadPart, buildUploadPartArgs("b", "k", "upload-id", int32(i+1), nil), bytes.NewReader([]byte("part")))
			done <- err
		}()
	}

	for i := 0; i < concurrency; i++ {
		select {
		case <-started:
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for concurrent streams to start")
		}
	}
}

func TestForwardSender_SendStream_BackpressureLimit(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release
		_, _ = io.Copy(io.Discard, body)
		return okReplyBytes(t), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
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

func TestForwardSender_SendReadStreamUsesSeparateSlotPool(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		once.Do(func() { close(started) })
		<-release
		_, _ = io.Copy(io.Discard, body)
		return okReplyBytes(t), nil
	}
	readDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		return okReplyBytes(t), io.NopCloser(bytes.NewReader([]byte("body"))), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		return okReplyBytes(t), nil
	}).WithStreamDialer(streamDialer).
		WithReadStreamDialer(readDialer).
		WithMaxForwardStreams(1).
		WithMaxForwardReadStreams(1)

	done := make(chan error, 1)
	go func() {
		_, err := s.SendStream(context.Background(), []string{"peer-a"}, "g",
			raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil), bytes.NewReader([]byte("first")))
		done <- err
	}()
	<-started

	reply, rc, err := s.SendReadStream(context.Background(), []string{"peer-a"}, "g",
		raftpb.ForwardOpGetObject, buildGetObjectArgs("b", "k"))
	require.NoError(t, err)
	require.NotNil(t, reply)
	require.NoError(t, rc.Close())

	close(release)
	require.NoError(t, <-done)
}

func TestForwardSender_SendReadStreamStaleLeaderHintTriesRemainingPeer(t *testing.T) {
	var connected []string
	readDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		connected = append(connected, peer)
		switch peer {
		case "peer-a":
			return notLeaderReplyBytes(t, "dead-leader"), io.NopCloser(bytes.NewReader(nil)), nil
		case "dead-leader":
			return nil, nil, errors.New("connection refused")
		case "peer-b":
			return okReplyBytes(t), io.NopCloser(bytes.NewReader([]byte("body"))), nil
		default:
			return nil, nil, errors.New("unexpected peer")
		}
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		return nil, errors.New("single-message dialer unused")
	}).WithReadStreamDialer(readDialer)

	reply, rc, err := s.SendReadStream(context.Background(), []string{"peer-a", "peer-b"}, "g",
		raftpb.ForwardOpGetObject, buildGetObjectArgs("b", "k"))

	require.NoError(t, err)
	require.NotNil(t, reply)
	require.NoError(t, rc.Close())
	require.Equal(t, []string{"peer-a", "dead-leader", "peer-b"}, connected)
}

func TestForwardSender_SendReadStreamRetriesNotLeaderWithinCallerDeadline(t *testing.T) {
	var connected []string
	readDialer := func(ctx context.Context, peer string, payload []byte) ([]byte, io.ReadCloser, error) {
		connected = append(connected, peer)
		if len(connected) < 3 {
			return notLeaderReplyBytes(t, ""), io.NopCloser(bytes.NewReader(nil)), nil
		}
		return okReplyBytes(t), io.NopCloser(bytes.NewReader([]byte("body"))), nil
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		return nil, errors.New("single-message dialer unused")
	}).WithReadStreamDialer(readDialer)
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	reply, rc, err := s.SendReadStream(ctx, []string{"peer-a", "peer-b"}, "g",
		raftpb.ForwardOpGetObject, buildGetObjectArgs("b", "k"))

	require.NoError(t, err)
	require.NotNil(t, reply)
	require.NoError(t, rc.Close())
	require.Equal(t, []string{"peer-a", "peer-b", "peer-a"}, connected)
}

func TestForwardSender_SendStreamUsesPerCallTimeoutContext(t *testing.T) {
	started := make(chan struct{})
	streamDialer := func(ctx context.Context, peer string, payload []byte, body io.Reader) ([]byte, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	s := NewForwardSender(func(context.Context, string, []byte) ([]byte, error) {
		t.Fatal("single-message dialer must not be used for streamed body")
		return nil, nil
	}).WithStreamDialer(streamDialer)
	s.timeout = 20 * time.Millisecond

	start := time.Now()
	_, err := s.SendStream(context.Background(), []string{"slow-peer"}, "g",
		raftpb.ForwardOpPutObject, buildPutObjectArgs("b", "k", "text/plain", nil), bytes.NewReader([]byte("payload")))

	require.ErrorIs(t, err, ErrNoReachablePeer)
	require.Less(t, time.Since(start), 200*time.Millisecond)
	<-started
}

func TestForwardSender_SendRecordsAttemptsAndNotLeaderRetry(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()

	calls := 0
	s := NewForwardSender(func(ctx context.Context, peer string, payload []byte) ([]byte, error) {
		calls++
		if calls == 1 {
			return notLeaderReplyBytes(t, "peer-b"), nil
		}
		return okReplyBytes(t), nil
	})

	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      "bench",
		Key:         "retry-key",
		GroupID:     "group-1",
		Ingress:     PutTraceIngressForwardedNonLeader,
		SizeClass:   PutTraceSizeSmall,
		ForwardMode: PutTraceForwardFrame,
	})
	_, err := s.Send(ctx, []string{"peer-a"}, "group-1", raftpb.ForwardOpHeadObject, headObjectArgsBytes(t, "bench", "retry-key"))
	require.NoError(t, err)

	events := readPutTraceEvents(t, path)
	requirePutTraceStage(t, events, PutTraceStageForwardNotLeaderRetry)
	var send PutTraceEvent
	for _, ev := range events {
		if ev.Stage == PutTraceStageForwardSendFrame {
			send = ev
		}
	}
	require.Equal(t, 2, send.ForwardAttempts)
	require.True(t, send.LeaderHintUsed)
}

func TestReadAtReplyIntoFullBuffer(t *testing.T) {
	reply := buildReadAtReply([]byte("abcdef"))
	dst := make([]byte, 6)

	n, err := readAtReplyInto(reply, dst)

	require.NoError(t, err)
	require.Equal(t, 6, n)
	require.Equal(t, []byte("abcdef"), dst)
}

func TestReadAtReplyIntoShortBodyReturnsEOF(t *testing.T) {
	reply := buildReadAtReply([]byte("tail"))
	dst := make([]byte, 128)

	n, err := readAtReplyInto(reply, dst)

	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, 4, n)
	require.Equal(t, []byte("tail"), dst[:n])
}

func TestReadAtReplyIntoRejectsOversizedBody(t *testing.T) {
	reply := buildReadAtReply([]byte("too-large"))
	dst := make([]byte, 3)

	n, err := readAtReplyInto(reply, dst)

	require.ErrorIs(t, err, ErrForwardBodySizeMismatch)
	require.Equal(t, 3, n)
	require.Equal(t, []byte("too"), dst)
}

func TestReadAtReplyIntoMalformedReplyReturnsError(t *testing.T) {
	dst := make([]byte, 8)

	n, err := readAtReplyInto([]byte{0x01, 0x02, 0x03}, dst)

	require.Zero(t, n)
	require.Error(t, err)
}
