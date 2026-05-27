package cluster

import "testing"

func spki(b byte) [32]byte { var s [32]byte; s[0] = b; return s }

func TestPeerRegistry_RegisterPromoteLookup(t *testing.T) {
	r := newPeerRegistry()
	if err := r.registerPendingLearner("node-a", spki(1), "10.0.0.2:9000"); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Same-package test reads the registry map directly (no production
	// lookupByNodeID accessor until Task 6 wires per-peer dial pinning).
	e, ok := r.byNodeID["node-a"]
	if !ok || e.State != peerStatePendingLearner {
		t.Fatal("expected pending-learner entry")
	}
	if err := r.promoteMember("node-a"); err != nil {
		t.Fatalf("promote: %v", err)
	}
	e = r.byNodeID["node-a"]
	if e.State != peerStateMember {
		t.Fatal("expected member state after promote")
	}
}

func TestPeerRegistry_SPKIUniqueness(t *testing.T) {
	r := newPeerRegistry()
	_ = r.registerPendingLearner("node-a", spki(1), "10.0.0.2:9000")
	if err := r.registerPendingLearner("node-b", spki(1), "10.0.0.3:9000"); err == nil {
		t.Fatal("duplicate SPKI accepted; want rejection")
	}
}

func TestPeerRegistry_Denylist(t *testing.T) {
	r := newPeerRegistry()
	r.denylist(spki(9))
	if !r.isDenylisted(spki(9)) {
		t.Fatal("denylisted SPKI not reported")
	}
	if err := r.registerPendingLearner("node-x", spki(9), "10.0.0.9:9000"); err == nil {
		t.Fatal("registering a denylisted SPKI must fail")
	}
}

func TestPeerRegistry_NodeIDRebindRejected(t *testing.T) {
	r := newPeerRegistry()
	if err := r.registerPendingLearner("node-a", spki(1), "10.0.0.2:9000"); err != nil {
		t.Fatalf("first register: %v", err)
	}
	// Same node-id, DIFFERENT SPKI (the hijack attempt) must be rejected.
	if err := r.registerPendingLearner("node-a", spki(2), "10.0.0.9:9000"); err == nil {
		t.Fatal("rebinding an existing node-id to a different SPKI must be rejected")
	}
	// Same node-id, SAME SPKI, new address (idempotent re-join) must still succeed.
	if err := r.registerPendingLearner("node-a", spki(1), "10.0.0.2:9001"); err != nil {
		t.Fatalf("idempotent re-register (same node+SPKI) must succeed: %v", err)
	}
	if r.byNodeID["node-a"].Address != "10.0.0.2:9001" {
		t.Fatal("address should refresh on idempotent re-register")
	}
}

func TestPeerRegistry_PromoteUnknownErrors(t *testing.T) {
	r := newPeerRegistry()
	if err := r.promoteMember("ghost"); err == nil {
		t.Fatal("promoting an unknown node must error")
	}
}

func TestPeerRegistry_AcceptSet(t *testing.T) {
	r := newPeerRegistry()
	_ = r.registerPendingLearner("node-a", spki(1), "10.0.0.2:9000")
	_ = r.registerPendingLearner("node-b", spki(2), "10.0.0.3:9000")
	set := r.acceptSPKIs()
	if len(set) != 2 {
		t.Fatalf("acceptSPKIs len %d, want 2 (learners are transport-accepted)", len(set))
	}
}
