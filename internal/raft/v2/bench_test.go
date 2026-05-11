package raftv2

// Benchmark harness for raft/v2 (PR 21 — M3 close).
//
// Methodology: standard Go testing.B, not k6. Run with:
//
//	go test -bench=. -benchmem -benchtime=2s -count=3 -run '^$' ./internal/raft/v2
//
// For the 1 GiB snapshot bench, use -benchtime=1x because setup dominates:
//
//	go test -bench=BenchmarkInstallSnapshot_1GiB -benchtime=1x -count=1 -run '^$' ./internal/raft/v2
//
// v1 vs v2 comparison: deferred. The redesign goal was safety/maintainability,
// not throughput (see plan §Driver). Absolute v2 numbers are recorded in
// CHANGELOG [0.0.135.0].

import (
	"context"
	"fmt"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

// BenchmarkProposeWait_SingleNode_NoFsync measures ProposeWait latency on a
// single-voter node backed by in-memory LogStore and StableStore (memLogStore
// + memStableStore). No I/O cost; measures pure actor-path overhead.
//
// Setup: default Config with no explicit stores (nil → mem defaults). Node
// auto-promotes to Leader on Start. ApplyCh is drained in a background
// goroutine so the actor's enqueue never blocks.
func BenchmarkProposeWait_SingleNode_NoFsync(b *testing.B) {
	n, err := NewNode(Config{ID: "n1"})
	if err != nil {
		b.Fatalf("NewNode: %v", err)
	}
	n.Start()
	b.Cleanup(n.Stop)

	if err := waitFor(2*time.Second, n.IsLeader); err != nil {
		b.Fatalf("node did not become leader: %v", err)
	}

	go func() {
		for range n.ApplyCh() {
		}
	}()

	cmd := []byte("bench-payload")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := n.ProposeWait(ctx, cmd); err != nil {
			b.Fatalf("ProposeWait: %v", err)
		}
	}
}

// BenchmarkProposeWait_SingleNode_BadgerDefault measures ProposeWait latency
// on a single-voter node backed by BadgerDB LogStore and StableStore on a
// real tmpdir.
//
// Durability note: badger.DefaultOptions has SyncWrites=false, but both
// badgerLogStore.Append and badgerStableStore.SaveHardState call db.Sync()
// explicitly after each write. Every committed entry is therefore power-loss
// durable before ProposeWait returns. This measures real-deployment commit
// latency including fsync cost on the local filesystem.
func BenchmarkProposeWait_SingleNode_BadgerDefault(b *testing.B) {
	dir := b.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	if err != nil {
		b.Fatalf("badger.Open: %v", err)
	}
	b.Cleanup(func() { _ = db.Close() })

	logStore, err := newBadgerLogStore(db, []byte("raft/v2/log/"))
	if err != nil {
		b.Fatalf("newBadgerLogStore: %v", err)
	}
	stable, err := newBadgerStableStore(db, []byte("raft/v2/hardstate/"))
	if err != nil {
		b.Fatalf("newBadgerStableStore: %v", err)
	}

	n, err := NewNode(Config{
		ID:          "n1",
		LogStore:    logStore,
		StableStore: stable,
	})
	if err != nil {
		b.Fatalf("NewNode: %v", err)
	}
	n.Start()
	b.Cleanup(n.Stop)

	if err := waitFor(2*time.Second, n.IsLeader); err != nil {
		b.Fatalf("node did not become leader: %v", err)
	}

	go func() {
		for range n.ApplyCh() {
		}
	}()

	cmd := []byte("bench-payload")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := n.ProposeWait(ctx, cmd); err != nil {
			b.Fatalf("ProposeWait: %v", err)
		}
	}
}

// BenchmarkProposeAndCommit_3Voter measures ProposeWait throughput on a
// 3-voter cluster backed by in-memory stores and wired through memNetwork
// (no real network). Measures actor + in-process RPC round-trip cost.
//
// Setup: n1 gets the fast election timeout so it wins deterministically.
// All three nodes' ApplyCh are drained in background goroutines. Timer
// resets after cluster stabilises (leader elected).
func BenchmarkProposeAndCommit_3Voter(b *testing.B) {
	ids := [3]string{"n1", "n2", "n3"}
	net := newMemNetwork()

	nodes := make([]*Node, 3)
	for i, id := range ids {
		peers := make([]string, 0, 2)
		for _, p := range ids {
			if p != id {
				peers = append(peers, p)
			}
		}
		et := slowElectionTimeout
		if i == 0 {
			et = fastElectionTimeout
		}
		n, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  et,
			HeartbeatTimeout: testHeartbeat,
		})
		if err != nil {
			b.Fatalf("NewNode %s: %v", id, err)
		}
		nodes[i] = n
	}

	for _, n := range nodes {
		n.SetTransport(net.Register(n.ID(), n))
	}
	for _, n := range nodes {
		n.Start()
		b.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}

	// Wait for a leader before resetting the timer.
	if err := waitFor(3*time.Second, nodes[0].IsLeader); err != nil {
		b.Fatalf("n1 did not become leader: %v", err)
	}
	leader := nodes[0]

	cmd := []byte("bench-payload")
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := leader.ProposeWait(ctx, cmd); err != nil {
			b.Fatalf("ProposeWait: %v", err)
		}
	}
}

// BenchmarkInstallSnapshot_1GiB measures the wall-clock time for a leader to
// send a 1 GiB snapshot to a lagging follower and for the follower to install
// it (LogEntrySnapshot delivered on follower's ApplyCh).
//
// Use -benchtime=1x because the 1 GiB allocation and leader-side snapshot
// creation dominate; repeating N>1 times adds no additional signal and
// exhausts memory on constrained machines.
//
//	go test -bench=BenchmarkInstallSnapshot_1GiB -benchtime=1x -count=1 -run '^$' ./internal/raft/v2
//
// Methodology: set up a 3-voter cluster (n1 fast leader, n3 follower, n2
// offline). Propose 10 entries to build a log on {n1, n3}. Leader compacts
// to index 8 so n2 will require InstallSnapshot on join. Populate snapshot
// with a 1 GiB Data payload. Start n2 and wait for LogEntrySnapshot on its
// ApplyCh. b.SetBytes reports throughput in MB/s.
func BenchmarkInstallSnapshot_1GiB(b *testing.B) {
	const snapSize = 1 << 30 // 1 GiB

	net := newMemNetwork()

	makeNode := func(id string, peers []string, fast bool) *Node {
		et := slowElectionTimeout
		if fast {
			et = fastElectionTimeout
		}
		n, err := NewNode(Config{
			ID:               id,
			Peers:            peers,
			ElectionTimeout:  et,
			HeartbeatTimeout: testHeartbeat,
		})
		if err != nil {
			b.Fatalf("NewNode %s: %v", id, err)
		}
		return n
	}

	n1 := makeNode("n1", []string{"n2", "n3"}, true)
	n2 := makeNode("n2", []string{"n1", "n3"}, false)
	n3 := makeNode("n3", []string{"n1", "n2"}, false)

	n1.SetTransport(net.Register("n1", n1))
	n2.SetTransport(net.Register("n2", n2))
	n3.SetTransport(net.Register("n3", n3))

	// Start n1 and n3 first; n2 stays offline.
	for _, n := range []*Node{n1, n3} {
		n.Start()
		b.Cleanup(n.Stop)
		go func(n *Node) {
			for range n.ApplyCh() {
			}
		}(n)
	}
	if err := waitFor(3*time.Second, n1.IsLeader); err != nil {
		b.Fatalf("n1 did not become leader: %v", err)
	}

	// Propose 10 entries (committed via {n1, n3} majority).
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := 0; i < 10; i++ {
		if _, err := n1.ProposeWait(ctx, []byte(fmt.Sprintf("entry-%d", i))); err != nil {
			b.Fatalf("ProposeWait entry-%d: %v", i, err)
		}
	}

	// Compact leader log to index 8, embedding a 1 GiB FSM state blob.
	snapData := make([]byte, snapSize)
	if err := n1.CreateSnapshot(8, snapData); err != nil {
		b.Fatalf("CreateSnapshot: %v", err)
	}
	snapData = nil // allow GC before the benchmark loop

	// Only a single iteration makes sense; we're measuring a one-shot transfer.
	b.ResetTimer()
	b.SetBytes(snapSize)

	for i := 0; i < b.N; i++ {
		// Start n2 (its log is empty; leader sends InstallSnapshot).
		n2applied := make(chan LogEntry, 64)
		go func() {
			for e := range n2.ApplyCh() {
				n2applied <- e
			}
		}()
		n2.Start()

		// Wait for the LogEntrySnapshot signal on n2.
		deadline := time.After(30 * time.Second)
	waitSnap:
		for {
			select {
			case e := <-n2applied:
				if e.Type == LogEntrySnapshot {
					break waitSnap
				}
			case <-deadline:
				b.Fatal("timeout waiting for LogEntrySnapshot on n2")
			}
		}

		// Stop n2 between iterations so each iteration starts fresh.
		n2.Stop()
		// Rebuild n2 for the next iteration (if b.N > 1 with -benchtime=1x it
		// won't loop, but we guard defensively).
		if i+1 < b.N {
			n2 = makeNode("n2", []string{"n1", "n3"}, false)
			n2.SetTransport(net.Register("n2", n2))
		}
	}
}
