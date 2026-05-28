package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func spki(b byte) [32]byte { var s [32]byte; s[0] = b; return s }

// lookupByNodeID is a test-only read accessor (the production registry has no
// node-id lookup yet; the revocation slice will add one when RevokeNode needs
// it). Kept in _test.go so it is not flagged as unused production code.
func (r *peerRegistry) lookupByNodeID(nodeID string) (peerEntry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.byNodeID[nodeID]
	return e, ok
}

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

func TestPeerRegistry_RemoveReturnsEntryAndDenylistRejectsRejoin(t *testing.T) {
	r := newPeerRegistry()
	s := spki(9)
	require.NoError(t, r.registerMember("node-9", s, "127.0.0.1:7009", true, 0))
	got, ok := r.remove("node-9")
	require.True(t, ok)
	require.Equal(t, s, got.SPKI)
	require.Equal(t, "127.0.0.1:7009", got.Address)

	r.denylist(got.SPKI)
	require.ErrorIs(t, r.registerMember("node-9", s, "127.0.0.1:7009", true, 0), errSPKIDenylisted)
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
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false, 0); err != nil {
		t.Fatalf("registerMember: %v", err)
	}
	if r.byNodeID["n"].State != peerStateMember {
		t.Fatal("registerMember must not demote an existing member")
	}
}

func TestPeerRegistry_RegisterMember_InsertsAsMember(t *testing.T) {
	r := newPeerRegistry()
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false, 0); err != nil {
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
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", true, 0); err != nil {
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
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false, 0); err != nil {
		t.Fatalf("first registerMember: %v", err)
	}
	if err := r.registerMember("n", spki(1), "10.0.0.2:9000", false, 0); err != nil {
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
	if err := r.registerMember("member-off", spki(2), "10.0.0.3:9000", false, 0); err != nil {
		t.Fatalf("registerMember(false): %v", err)
	}
	if r.byNodeID["member-off"].PresentsPerNode {
		t.Fatal("registerMember(false) must record PresentsPerNode=false")
	}

	// A member registered with presentsPerNode=true records true.
	if err := r.registerMember("member-on", spki(3), "10.0.0.4:9000", true, 0); err != nil {
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
			data, err := encodeRegisterMemberCmd(tc.nodeID, spki(tc.spkiByte), "10.0.0.5:9000", tc.presentsPerNode, 4)
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
	data, err := encodeRegisterMemberCmd("n", spki(1), "10.0.0.2:9000", true, 0)
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

func TestApplyRevokePeer_BurnsPendingInviteAndDenylistsRegisteredPeer(t *testing.T) {
	fsm := NewMetaFSM()
	now := time.Unix(10, 0)
	s := spki(12)
	fsm.invites.applyMint("inv-12", testPub(t), now.Add(time.Hour).UnixNano())
	require.NoError(t, fsm.invites.applyPending("inv-12", "node-12", s, "127.0.0.1:7012", now.UnixNano()))
	require.NoError(t, fsm.Peers().registerMember("node-12", s, "127.0.0.1:7012", true, 0))
	fired := 0
	fsm.SetOnPeersChanged(func([][32]byte) { fired++ })
	payload, err := encodeRevokePeerCmd("node-12")
	require.NoError(t, err)

	require.NoError(t, fsm.applyRevokePeer(payload))

	require.Equal(t, 1, fired)
	require.True(t, fsm.Peers().isDenylisted(s))
	require.ErrorIs(t, fsm.Peers().registerMember("node-12", s, "127.0.0.1:7012", true, 0), errSPKIDenylisted)
	_, ok := fsm.Peers().lookupByNodeID("node-12")
	require.False(t, ok)
	_, _, _, ok = fsm.invites.lookupPending("inv-12")
	require.False(t, ok)
	_, ok = fsm.invites.lookup("inv-12", now)
	require.False(t, ok)
}

func TestApplyRevokePeer_DenylistsAndBurnsPhase1OnlyPendingNode(t *testing.T) {
	fsm := NewMetaFSM()
	now := time.Unix(10, 0)
	s := spki(13)
	fsm.invites.applyMint("inv-13", testPub(t), now.Add(time.Hour).UnixNano())
	require.NoError(t, fsm.invites.applyPending("inv-13", "node-pending", s, "127.0.0.1:7013", now.UnixNano()))
	fired := 0
	fsm.SetOnPeersChanged(func([][32]byte) { fired++ })
	payload, err := encodeRevokePeerCmd("node-pending")
	require.NoError(t, err)

	require.NoError(t, fsm.applyRevokePeer(payload))

	require.Equal(t, 1, fired)
	require.True(t, fsm.Peers().isDenylisted(s))
	require.ErrorIs(t, fsm.Peers().registerMember("node-pending", s, "127.0.0.1:7013", true, 0), errSPKIDenylisted)
	_, _, _, ok := fsm.invites.lookupPending("inv-13")
	require.False(t, ok)
	_, ok = fsm.invites.lookup("inv-13", now)
	require.False(t, ok)
}

func TestApplyRevokePeer_UnknownNodeIsIdempotentNoCallback(t *testing.T) {
	fsm := NewMetaFSM()
	fired := 0
	fsm.SetOnPeersChanged(func([][32]byte) { fired++ })
	payload, err := encodeRevokePeerCmd("ghost")
	require.NoError(t, err)

	require.NoError(t, fsm.applyRevokePeer(payload))

	require.Equal(t, 0, fired)
}

func TestImportEntries_RejectsDuplicateAndMalformed(t *testing.T) {
	good := func() []peerEntry {
		return []peerEntry{
			{NodeID: "n1", SPKI: spki(1), Address: "10.0.0.1:9000", State: peerStateMember},
			{NodeID: "n2", SPKI: spki(2), Address: "10.0.0.2:9000", State: peerStatePendingLearner},
		}
	}

	cases := []struct {
		name    string
		entries []peerEntry
		wantErr string
	}{
		{
			name:    "valid entries import cleanly",
			entries: good(),
			wantErr: "",
		},
		{
			name: "zero/malformed SPKI (31-byte copy zero-pads)",
			entries: []peerEntry{
				{NodeID: "n1", SPKI: [32]byte{}, Address: "a", State: peerStateMember},
			},
			wantErr: "zero/malformed SPKI",
		},
		{
			name: "two entries same SPKI different node-id",
			entries: []peerEntry{
				{NodeID: "n1", SPKI: spki(7), Address: "a", State: peerStateMember},
				{NodeID: "n2", SPKI: spki(7), Address: "b", State: peerStateMember},
			},
			wantErr: "duplicate SPKI",
		},
		{
			name: "two entries same node-id",
			entries: []peerEntry{
				{NodeID: "n1", SPKI: spki(1), Address: "a", State: peerStateMember},
				{NodeID: "n1", SPKI: spki(2), Address: "b", State: peerStateMember},
			},
			wantErr: "duplicate node ID",
		},
		{
			name: "empty node ID",
			entries: []peerEntry{
				{NodeID: "", SPKI: spki(1), Address: "a", State: peerStateMember},
			},
			wantErr: "empty node ID",
		},
		{
			name: "invalid state enum",
			entries: []peerEntry{
				{NodeID: "n1", SPKI: spki(1), Address: "a", State: peerState(99)},
			},
			wantErr: "invalid state",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newPeerRegistry()
			// Seed prior state so we can assert it is untouched on error (validate
			// BEFORE mutating — no half-cleared registry). validatePeerEntries only
			// BUILDS local indexes; commitPeerIndexes swaps them in.
			seedByNode, seedBySPKI, err := validatePeerEntries([]peerEntry{{NodeID: "seed", SPKI: spki(200), Address: "s", State: peerStateMember}})
			if err != nil {
				t.Fatalf("seed validate: %v", err)
			}
			r.commitPeerIndexes(seedByNode, seedBySPKI)

			byNodeID, bySPKI, err := validatePeerEntries(tc.entries)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("want success, got error: %v", err)
				}
				r.commitPeerIndexes(byNodeID, bySPKI)
				// Consistency: no SPKI maps to two node-ids.
				if len(r.bySPKI) != len(tc.entries) {
					t.Fatalf("bySPKI len %d, want %d", len(r.bySPKI), len(tc.entries))
				}
				for s, owner := range r.bySPKI {
					if got := r.byNodeID[owner].SPKI; got != s {
						t.Fatalf("bySPKI[%x]=%s but byNodeID[%s].SPKI=%x (inconsistent)", s[:2], owner, owner, got[:2])
					}
				}
				return
			}
			if err == nil {
				t.Fatalf("want error containing %q, got nil", tc.wantErr)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErr)
			}
			// On a validation error the registry must be unchanged (validate never
			// touched it — seed still present, no malformed entry leaked in).
			if _, ok := r.lookupByNodeID("seed"); !ok {
				t.Fatal("registry was mutated on error: seed entry lost")
			}
			if len(r.byNodeID) != 1 {
				t.Fatalf("registry mutated on error: byNodeID len %d, want 1 (seed only)", len(r.byNodeID))
			}
		})
	}
}

func TestValidateDenylistEntries_RejectsDuplicateAndMalformed(t *testing.T) {
	valid, err := validateDenylistEntries([][32]byte{spki(1), spki(2)})
	require.NoError(t, err)
	require.Len(t, valid, 2)

	_, err = validateDenylistEntries([][32]byte{{}})
	require.ErrorContains(t, err, "zero/malformed SPKI")

	_, err = validateDenylistEntries([][32]byte{spki(3), spki(3)})
	require.ErrorContains(t, err, "duplicate SPKI")
}

func TestPeerRegistry_ExportCommitDenylist(t *testing.T) {
	r := newPeerRegistry()
	r.denylist(spki(4))
	r.denylist(spki(5))

	deny, err := validateDenylistEntries(r.exportDenylist())
	require.NoError(t, err)
	dst := newPeerRegistry()
	dst.commitDenylist(deny)

	require.True(t, dst.isDenylisted(spki(4)))
	require.True(t, dst.isDenylisted(spki(5)))
	require.False(t, dst.isDenylisted(spki(6)))
}

func TestRegisterMember_PresentsPerNode_Monotone(t *testing.T) {
	r := newPeerRegistry()
	var spki [32]byte
	spki[0] = 0xAB
	require.NoError(t, r.registerMember("n1", spki, "addr1", true, 0))  // recorded true
	require.NoError(t, r.registerMember("n1", spki, "addr1", false, 0)) // restart self-register: false
	require.True(t, r.byNodeID["n1"].PresentsPerNode, "presents_per_node monotone: true must not regress")
}

func TestRegisterMember_PresentsPerNode_FalseToTrue(t *testing.T) {
	r := newPeerRegistry()
	var spki [32]byte
	spki[0] = 0xCD
	require.NoError(t, r.registerMember("n2", spki, "addr2", false, 0))
	require.NoError(t, r.registerMember("n2", spki, "addr2", true, 0)) // false->true applies
	require.True(t, r.byNodeID["n2"].PresentsPerNode)
}

func TestRegisterMember_NodeKeyKEKGen_Monotone(t *testing.T) {
	r := newPeerRegistry()
	var spki [32]byte
	spki[0] = 0xEF
	require.NoError(t, r.registerMember("n3", spki, "addr3", true, 7))
	require.NoError(t, r.registerMember("n3", spki, "addr3", true, 5))
	require.Equal(t, uint32(7), r.byNodeID["n3"].NodeKeyKEKGen, "node_key_kek_gen must not regress")
	require.NoError(t, r.registerMember("n3", spki, "addr3", true, 9))
	require.Equal(t, uint32(9), r.byNodeID["n3"].NodeKeyKEKGen, "newer node_key_kek_gen must apply")
}

func TestValidateVoterNodeKeyKEKGens(t *testing.T) {
	r := newPeerRegistry()
	require.NoError(t, r.registerMember("node-A", [32]byte{0xAA}, "127.0.0.1:1001", true, 4))
	require.NoError(t, r.registerMember("node-B", [32]byte{0xBB}, "127.0.0.1:1002", true, 2))

	require.NoError(t, r.validateVoterNodeKeyKEKGens([]string{"127.0.0.1:1001"}, 3))
	require.Error(t, r.validateVoterNodeKeyKEKGens([]string{"127.0.0.1:1002"}, 3))
	require.Error(t, r.validateVoterNodeKeyKEKGens([]string{"127.0.0.1:1003"}, 3))
}

func TestAllVotersPresentsPerNode_TrueWhenAll(t *testing.T) {
	r := newPeerRegistry()
	require.NoError(t, r.registerMember("node-A", [32]byte{0xAA}, "127.0.0.1:1001", true, 0))
	require.NoError(t, r.registerMember("node-B", [32]byte{0xBB}, "127.0.0.1:1002", true, 0))
	require.True(t, r.allVotersPresentsPerNode([]string{"127.0.0.1:1001", "127.0.0.1:1002"}))
}

func TestAllVotersPresentsPerNode_FalseWhenOneMissing(t *testing.T) {
	r := newPeerRegistry()
	require.NoError(t, r.registerMember("node-A", [32]byte{0xAA}, "127.0.0.1:1001", true, 0))
	require.NoError(t, r.registerMember("node-B", [32]byte{0xBB}, "127.0.0.1:1002", false, 0))
	require.False(t, r.allVotersPresentsPerNode([]string{"127.0.0.1:1001", "127.0.0.1:1002"}))
}

func TestAllVotersPresentsPerNode_FalseWhenVoterUnregistered(t *testing.T) {
	r := newPeerRegistry()
	require.NoError(t, r.registerMember("node-A", [32]byte{0xAA}, "127.0.0.1:1001", true, 0))
	require.False(t, r.allVotersPresentsPerNode([]string{"127.0.0.1:1001", "127.0.0.1:1002"}))
}
