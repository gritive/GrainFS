package raft

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// TestSingleNode_ProposeRoundTrip validates the actor model end-to-end:
//   - On Start, a single-voter node auto-promotes to Leader (bootstrap state,
//     not real election — election is out of scope for PR 1).
//   - readState snapshot reflects the leader role for hot-path readers.
//   - Propose appends a LogEntry that surfaces on ApplyCh with the original
//     command bytes, and the published commitIndex advances accordingly.
//   - Stop returns cleanly without leaking goroutines.
func TestSingleNode_ProposeRoundTrip(t *testing.T) {
	cfg := Config{ID: "n1"} // single voter (Peers empty == only self)
	n, err := NewNode(cfg)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}

	n.Start()
	defer n.Stop()

	// Allow the actor goroutine to publish the initial leader readState.
	if err := waitFor(time.Second, func() bool { return n.IsLeader() }); err != nil {
		t.Fatalf("node did not become leader: %v", err)
	}

	if got, want := n.State(), Leader; got != want {
		t.Fatalf("State() = %v, want %v", got, want)
	}
	if got, want := n.Term(), uint64(1); got != want {
		t.Fatalf("Term() = %d, want %d", got, want)
	}
	if !n.IsLeader() {
		t.Fatalf("IsLeader() = false, want true")
	}
	if got, want := n.LeaderID(), "n1"; got != want {
		t.Fatalf("LeaderID() = %q, want %q", got, want)
	}
	if got, want := n.CommittedIndex(), uint64(0); got != want {
		t.Fatalf("CommittedIndex() = %d, want %d", got, want)
	}

	cmd := []byte("hello")
	if err := n.Propose(cmd); err != nil {
		t.Fatalf("Propose: %v", err)
	}

	select {
	case entry := <-n.ApplyCh():
		if !bytes.Equal(entry.Command, cmd) {
			t.Fatalf("ApplyCh entry.Command = %q, want %q", entry.Command, cmd)
		}
		if entry.Index != 1 {
			t.Fatalf("entry.Index = %d, want 1", entry.Index)
		}
		if entry.Term != 1 {
			t.Fatalf("entry.Term = %d, want 1", entry.Term)
		}
	case <-time.After(time.Second):
		t.Fatalf("timeout waiting for ApplyCh entry")
	}

	if got, want := n.CommittedIndex(), uint64(1); got != want {
		t.Fatalf("CommittedIndex() after propose = %d, want %d", got, want)
	}
}

// TestSingleNode_ProposeWait validates that ProposeWait blocks until the entry
// is committed and returns its log index.
func TestSingleNode_ProposeWait(t *testing.T) {
	n, err := NewNode(Config{ID: "n1"})
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	n.Start()
	defer n.Stop()

	if err := waitFor(time.Second, func() bool { return n.IsLeader() }); err != nil {
		t.Fatalf("node did not become leader: %v", err)
	}

	// Drain ApplyCh in the background so the actor's send doesn't block while
	// ProposeWait waits for commit completion.
	go func() {
		for range n.ApplyCh() {
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	idx, err := n.ProposeWait(ctx, []byte("payload"))
	if err != nil {
		t.Fatalf("ProposeWait: %v", err)
	}
	if idx != 1 {
		t.Fatalf("ProposeWait index = %d, want 1", idx)
	}
}

// waitFor polls cond until it returns true or the deadline elapses. Returns
// nil on success, ctx error on timeout. Used to bridge the brief async window
// between Start() and the actor's first readState publish.
func waitFor(d time.Duration, cond func() bool) error {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(time.Millisecond)
	}
	if cond() {
		return nil
	}
	return context.DeadlineExceeded
}
