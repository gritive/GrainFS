package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestMetaRaftCompleteCutoverUsesRaftIDAsSelf(t *testing.T) {
	m := &MetaRaft{cfg: MetaRaftConfig{NodeID: "node-a", RaftID: "127.0.0.1:7001"}}
	if got := m.cutoverSelfID(); got != "127.0.0.1:7001" {
		t.Fatalf("cutoverSelfID()=%q want raft ID", got)
	}

	m = &MetaRaft{cfg: MetaRaftConfig{NodeID: "node-a"}}
	if got := m.cutoverSelfID(); got != "node-a" {
		t.Fatalf("cutoverSelfID()=%q want node ID fallback", got)
	}
}

func TestMetaRaftCompleteCutoverRequiresProbeDialer(t *testing.T) {
	m := &MetaRaft{cfg: MetaRaftConfig{NodeID: "node-a", RaftID: "node-a"}}
	err := m.CompleteCutover(context.Background(), nil, time.Second)
	if err == nil {
		t.Fatal("CompleteCutover without dialer must fail")
	}
}

func TestWaitAllVotersPresentPerNodeWaitsUntilReady(t *testing.T) {
	var returned atomic.Bool
	go func() {
		time.Sleep(30 * time.Millisecond)
		returned.Store(true)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := waitAllVotersPresentPerNode(ctx,
		func() ([]string, uint64) { return []string{"node-a", "node-b"}, 7 },
		func([]string) bool { return returned.Load() },
		5*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("waitAllVotersPresentPerNode: %v", err)
	}
}

func TestWaitAllVotersPresentPerNodeTimesOut(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	err := waitAllVotersPresentPerNode(ctx,
		func() ([]string, uint64) { return []string{"node-a", "node-b"}, 7 },
		func([]string) bool { return false },
		2*time.Millisecond,
	)
	if err == nil {
		t.Fatal("expected timeout")
	}
}
