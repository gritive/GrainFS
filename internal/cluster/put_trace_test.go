package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPutTraceDisabledIsNoop(t *testing.T) {
	t.Setenv("GRAINFS_PUT_TRACE_FILE", "")
	reloadPutTraceSinkForTest()

	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:    "bench",
		Key:       "obj-disabled",
		GroupID:   "group-1",
		Ingress:   PutTraceIngressLocalLeader,
		SizeClass: PutTraceSizeSmall,
	})
	done := StartPutTraceStage(ctx, PutTraceStageRouteWrite)
	done(PutTraceStageFields{Bytes: 12})

	require.False(t, putTraceEnabled(), "trace must stay disabled unless GRAINFS_PUT_TRACE_FILE is set")
}

func TestPutTraceStageVocabularyIsStable(t *testing.T) {
	require.Equal(t, "http_put_total", string(PutTraceStageHTTPPutTotal))
	require.Equal(t, "http_put_prepare", string(PutTraceStageHTTPPutPrepare))
	require.Equal(t, "http_put_backend", string(PutTraceStageHTTPPutBackend))
	require.Equal(t, "http_put_mutation", string(PutTraceStageHTTPPutMutation))
	require.Equal(t, "http_put_response", string(PutTraceStageHTTPPutResponse))
	require.Equal(t, "route_write", string(PutTraceStageRouteWrite))
	require.Equal(t, "forward_resolve_leader", string(PutTraceStageForwardResolveLeader))
	require.Equal(t, "forward_send_frame", string(PutTraceStageForwardSendFrame))
	require.Equal(t, "forward_send_stream", string(PutTraceStageForwardSendStream))
	require.Equal(t, "forward_not_leader_retry", string(PutTraceStageForwardNotLeaderRetry))
	require.Equal(t, "forward_receiver_dispatch", string(PutTraceStageForwardReceiverDispatch))
	require.Equal(t, "receiver_backend_put", string(PutTraceStageReceiverBackendPut))
	require.Equal(t, "shard_write_local", string(PutTraceStageShardWriteLocal))
	require.Equal(t, "shard_write_remote", string(PutTraceStageShardWriteRemote))
	require.Equal(t, "shard_write_remote_open", string(PutTraceStageShardWriteRemoteOpen))
	require.Equal(t, "shard_write_remote_buffer", string(PutTraceStageShardWriteRemoteBuffer))
	require.Equal(t, "shard_write_remote_rpc", string(PutTraceStageShardWriteRemoteRPC))
	require.Equal(t, "shard_write_remote_build", string(PutTraceStageShardWriteRemoteBuild))
	require.Equal(t, "shard_write_remote_call", string(PutTraceStageShardWriteRemoteCall))
	require.Equal(t, "shard_write_remote_decode", string(PutTraceStageShardWriteRemoteDecode))
	require.Equal(t, "shard_write_local_mkdir", string(PutTraceStageShardWriteLocalMkdir))
	require.Equal(t, "shard_write_local_encode", string(PutTraceStageShardWriteLocalEncode))
	require.Equal(t, "shard_write_local_file", string(PutTraceStageShardWriteLocalFile))
	require.Equal(t, "shard_write_local_dirsync", string(PutTraceStageShardWriteLocalDirSync))
	require.Equal(t, "data_raft_propose_meta", string(PutTraceStageDataRaftProposeMeta))
	require.Equal(t, "meta_index_propose", string(PutTraceStageMetaIndexPropose))
}

func TestPutTraceWritesJSONLWhenEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	t.Setenv("GRAINFS_NODE_ID", "node-a")
	reloadPutTraceSinkForTest()

	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      "bench",
		Key:         "obj-json",
		GroupID:     "group-1",
		Ingress:     PutTraceIngressForwardedNonLeader,
		SizeClass:   PutTraceSizeLarge,
		ForwardMode: PutTraceForwardStream,
	})
	done := StartPutTraceStage(ctx, PutTraceStageForwardSendStream)
	done(PutTraceStageFields{
		Bytes:            6 << 20,
		ForwardAttempts:  2,
		LeaderHintUsed:   true,
		MetaProposeSite:  "coordinator",
		MetaProposeCount: 1,
	})

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	sc := bufio.NewScanner(f)
	require.True(t, sc.Scan())

	var ev PutTraceEvent
	require.NoError(t, json.Unmarshal(sc.Bytes(), &ev))
	require.Equal(t, "node-a", ev.NodeID)
	require.Equal(t, "bench", ev.Bucket)
	require.Equal(t, "obj-json", ev.Key)
	require.Equal(t, "group-1", ev.GroupID)
	require.Equal(t, PutTraceStageForwardSendStream, ev.Stage)
	require.Equal(t, PutTraceIngressForwardedNonLeader, ev.Ingress)
	require.Equal(t, PutTraceSizeLarge, ev.SizeClass)
	require.Equal(t, PutTraceForwardStream, ev.ForwardMode)
	require.Equal(t, int64(6<<20), ev.Bytes)
	require.Equal(t, 2, ev.ForwardAttempts)
	require.True(t, ev.LeaderHintUsed)
	require.Equal(t, "coordinator", ev.MetaProposeSite)
	require.Equal(t, 1, ev.MetaProposeCount)
	require.GreaterOrEqual(t, ev.DurationMicros, int64(0))
	require.NoError(t, sc.Err())
}

func TestPutTraceCreatesFilePrivateToOwner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()

	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:    "bench",
		Key:       "obj-private",
		GroupID:   "group-1",
		Ingress:   PutTraceIngressLocalLeader,
		SizeClass: PutTraceSizeSmall,
	})
	done := StartPutTraceStage(ctx, PutTraceStageRouteWrite)
	done(PutTraceStageFields{})

	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

func TestPutTraceConcurrentWritesRemainLineDelimited(t *testing.T) {
	path := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", path)
	reloadPutTraceSinkForTest()

	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:    "bench",
		Key:       "obj-concurrent",
		GroupID:   "group-1",
		Ingress:   PutTraceIngressLocalLeader,
		SizeClass: PutTraceSizeSmall,
	})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			done := StartPutTraceStage(ctx, PutTraceStageRouteWrite)
			time.Sleep(time.Microsecond)
			done(PutTraceStageFields{})
		}()
	}
	wg.Wait()

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	sc := bufio.NewScanner(f)
	lines := 0
	for sc.Scan() {
		lines++
		var ev PutTraceEvent
		require.NoError(t, json.Unmarshal(sc.Bytes(), &ev))
	}
	require.NoError(t, sc.Err())
	require.Equal(t, 20, lines)
}
