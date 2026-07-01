package cluster

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/transport"
)

// TestMeasureRaftRPCLatency is the N4 retune measurement tool (TODOS: "retune
// for warm pooled HTTP POST + one retry" — decide only with data). It measures
// the exact span raftRPCTimeout/metaRaftRPCTimeout bound: FB envelope encode →
// warm pooled HTTP POST round-trip on the buffered raft route → reply decode,
// against a receiver that performs the real inbound codec work.
//
// Gated: GRAINFS_MEASURE_RAFT_RPC=1 go test ./internal/cluster/ -run TestMeasureRaftRPCLatency -v -count=1
//
// macOS numbers are a sanity lower bound only; the retune decision itself needs
// the same tool run on Linux (the QUIC→TCP epic established that is where the
// perf signal lives).
func TestMeasureRaftRPCLatency(t *testing.T) {
	if os.Getenv("GRAINFS_MEASURE_RAFT_RPC") == "" {
		t.Skip("measurement tool; set GRAINFS_MEASURE_RAFT_RPC=1 to run")
	}
	ctx := context.Background()

	server := transport.MustNewHTTPTransport("measure-psk")
	require.NoError(t, server.Listen(ctx, "127.0.0.1:0"))
	defer server.Close()
	client := transport.MustNewHTTPTransport("measure-psk")
	require.NoError(t, client.Listen(ctx, "127.0.0.1:0"))
	defer client.Close()

	// Receiver mirrors RaftRPCTransport.handleRPC's codec work without the raft
	// state machine: decode envelope + args, encode a reply envelope.
	server.RegisterBufferedRoute(transport.RouteRaftGroupRPC, func(payload []byte) ([]byte, error) {
		rpcType, data, err := decodeRPC(payload)
		if err != nil {
			return nil, err
		}
		if rpcType != rpcTypeAppendEntries {
			return nil, fmt.Errorf("unexpected rpc type %s", rpcType)
		}
		if _, err := decodeAppendEntriesArgs(data); err != nil {
			return nil, err
		}
		return encodeRPC(rpcTypeAppendEntriesReply, &raft.AppendEntriesReply{Term: 1, Success: true})
	})
	addr := server.LocalAddr()

	roundTrip := func(args *raft.AppendEntriesArgs) time.Duration {
		start := time.Now()
		envelope, err := encodeRPC(rpcTypeAppendEntries, args)
		require.NoError(t, err)
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		reply, err := client.CallBuffered(cctx, addr, transport.RouteRaftGroupRPC, envelope)
		cancel()
		require.NoError(t, err)
		rpcType, data, err := decodeRPC(reply)
		require.NoError(t, err)
		require.Equal(t, rpcTypeAppendEntriesReply, rpcType)
		_, err = decodeAppendEntriesReply(data)
		require.NoError(t, err)
		return time.Since(start)
	}

	heartbeat := &raft.AppendEntriesArgs{Term: 1, LeaderID: "leader", LeaderCommit: 42}
	bulkEntries := make([]raft.LogEntry, 100)
	for i := range bulkEntries {
		bulkEntries[i] = raft.LogEntry{Term: 1, Index: uint64(i + 1), Command: make([]byte, 1024)}
	}
	bulk := &raft.AppendEntriesArgs{Term: 1, LeaderID: "leader", Entries: bulkEntries}

	// Warm the pool past cold-start (TLS handshake, conn establishment).
	for i := 0; i < 200; i++ {
		roundTrip(heartbeat)
	}

	report := func(name string, samples []time.Duration) {
		sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
		pct := func(p float64) time.Duration { return samples[int(float64(len(samples)-1)*p)] }
		over80, over500 := 0, 0
		for _, s := range samples {
			if s > 80*time.Millisecond {
				over80++
			}
			if s > 500*time.Millisecond {
				over500++
			}
		}
		t.Logf("%-28s n=%-6d p50=%-10v p90=%-10v p99=%-10v p99.9=%-10v max=%-10v >80ms=%d >500ms=%d",
			name, len(samples), pct(0.50), pct(0.90), pct(0.99), pct(0.999), samples[len(samples)-1], over80, over500)
	}

	// 1. Idle heartbeat (the raftRPCTimeout=80ms case: RequestVote/heartbeat-AE).
	idleHB := make([]time.Duration, 0, 5000)
	for i := 0; i < 5000; i++ {
		idleHB = append(idleHB, roundTrip(heartbeat))
	}
	report("idle heartbeat", idleHB)

	// 2. Idle bulk entries-AE (100×1KiB — replication batches).
	idleBulk := make([]time.Duration, 0, 1000)
	for i := 0; i < 1000; i++ {
		idleBulk = append(idleBulk, roundTrip(bulk))
	}
	report("idle bulk 100x1KiB", idleBulk)

	// 3. Heartbeat under contention: 8 goroutines hammer bulk RPCs while the
	// measured goroutine sends heartbeats — the CPU/conn-pool contention case
	// that originally forced metaRaftRPCTimeout up to 500ms.
	stop := make(chan struct{})
	var hammerCount atomic.Int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env, _ := encodeRPC(rpcTypeAppendEntries, bulk)
			for {
				select {
				case <-stop:
					return
				default:
				}
				cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				_, _ = client.CallBuffered(cctx, addr, transport.RouteRaftGroupRPC, env)
				cancel()
				hammerCount.Add(1)
			}
		}()
	}
	loadHB := make([]time.Duration, 0, 3000)
	for i := 0; i < 3000; i++ {
		loadHB = append(loadHB, roundTrip(heartbeat))
	}
	close(stop)
	wg.Wait()
	report("heartbeat under 8x bulk load", loadHB)
	t.Logf("background bulk RPCs completed during load phase: %d", hammerCount.Load())
}
