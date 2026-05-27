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

func TestPeerRegistry_RegisterMember_NeverDemotes(t *testing.T) {
	r := newPeerRegistry()
	if err := r.registerPendingLearner("n", spki(1), "10.0.0.2:9000"); err != nil {
		t.Fatalf("registerPendingLearner: %v", err)
	}
	if err := r.promoteMember("n"); err != nil {
		t.Fatalf("promoteMember: %v", err)
	}
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false); err != nil {
		t.Fatalf("registerMember: %v", err)
	}
	if r.byNodeID["n"].State != peerStateMember {
		t.Fatal("registerMember must not demote an existing member")
	}
}

func TestPeerRegistry_RegisterMember_InsertsAsMember(t *testing.T) {
	r := newPeerRegistry()
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false); err != nil {
		t.Fatalf("registerMember: %v", err)
	}
	e, ok := r.byNodeID["n"]
	if !ok || e.State != peerStateMember {
		t.Fatal("registerMember on absent node must insert as member")
	}
}

func TestPeerRegistry_RegisterMember_PromotesPendingLearner(t *testing.T) {
	r := newPeerRegistry()
	if err := r.registerPendingLearner("n", spki(1), "10.0.0.2:9000"); err != nil {
		t.Fatalf("registerPendingLearner: %v", err)
	}
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", true); err != nil {
		t.Fatalf("registerMember: %v", err)
	}
	e := r.byNodeID["n"]
	if e.State != peerStateMember {
		t.Fatal("registerMember must upgrade a pending-learner to member")
	}
	if !e.PresentsPerNode {
		t.Fatal("registerMember must record presents_per_node")
	}
}

func TestPeerRegistry_RegisterMember_Idempotent(t *testing.T) {
	r := newPeerRegistry()
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false); err != nil {
		t.Fatalf("first registerMember: %v", err)
	}
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false); err != nil {
		t.Fatalf("second registerMember (idempotent): %v", err)
	}
	if len(r.byNodeID) != 1 {
		t.Fatalf("idempotent re-register must yield a single member, got %d", len(r.byNodeID))
	}
	if r.byNodeID["n"].State != peerStateMember {
		t.Fatal("idempotent re-register must keep member state")
	}
}

// TestPeerEntry_PresentsPerNode_DefaultsFalseAndPersists confirms the
// presents_per_node readiness bit is recordable end-to-end (D-rev4). It is
// recording-only this slice; the revocation PSK-drop gate reads it later.
func TestPeerEntry_PresentsPerNode_DefaultsFalseAndPersists(t *testing.T) {
	// A pending-learner has no presents_per_node concept; defaults false.
	r := newPeerRegistry()
	if err := r.registerPendingLearner("learner", spki(1), "10.0.0.2:9000"); err != nil {
		t.Fatalf("registerPendingLearner: %v", err)
	}
	if r.byNodeID["learner"].PresentsPerNode {
		t.Fatal("pending-learner must default PresentsPerNode=false")
	}

	// A member registered with presentsPerNode=false records false.
	if err := r.registerMember("member-off", spki(2), "10.0.0.3:9000", false); err != nil {
		t.Fatalf("registerMember(false): %v", err)
	}
	if r.byNodeID["member-off"].PresentsPerNode {
		t.Fatal("registerMember(false) must record PresentsPerNode=false")
	}

	// A member registered with presentsPerNode=true records true.
	if err := r.registerMember("member-on", spki(3), "10.0.0.4:9000", true); err != nil {
		t.Fatalf("registerMember(true): %v", err)
	}
	if !r.byNodeID["member-on"].PresentsPerNode {
		t.Fatal("registerMember(true) must record PresentsPerNode=true")
	}

	// Apply path: decode → applyRegisterMember must carry the bit verbatim.
	for _, tc := range []struct {
		name            string
		nodeID          string
		spkiByte        byte
		presentsPerNode bool
	}{
		{"apply-true", "apply-on", 4, true},
		{"apply-false", "apply-off", 5, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fsm := NewMetaFSM()
			data, err := encodeRegisterMemberCmd(tc.nodeID, spki(tc.spkiByte), "10.0.0.5:9000", tc.presentsPerNode)
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if err := fsm.applyRegisterMember(data); err != nil {
				t.Fatalf("applyRegisterMember: %v", err)
			}
			if got := fsm.Peers().byNodeID[tc.nodeID].PresentsPerNode; got != tc.presentsPerNode {
				t.Fatalf("PresentsPerNode via apply path = %v, want %v", got, tc.presentsPerNode)
			}
		})
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

func TestApplyRegisterMember_FiresOnPeersChanged(t *testing.T) {
	fsm := NewMetaFSM()
	var got [][32]byte
	fired := 0
	fsm.SetOnPeersChanged(func(set [][32]byte) {
		fired++
		got = set
	})
	data, err := encodeRegisterMemberCmd("n", spki(1), "10.0.0.2:9000", true)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	if err := fsm.applyRegisterMember(data); err != nil {
		t.Fatalf("applyRegisterMember: %v", err)
	}
	if fired != 1 {
		t.Fatalf("onPeersChanged fired %d times, want 1", fired)
	}
	if len(got) != 1 {
		t.Fatalf("accept-set len %d, want 1", len(got))
	}
	if fsm.Peers().byNodeID["n"].State != peerStateMember {
		t.Fatal("applied RegisterMember must register as member")
	}
}
