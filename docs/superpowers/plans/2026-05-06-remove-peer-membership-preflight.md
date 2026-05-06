# Remove-Peer Membership Preflight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Route `remove-peer` quorum safety through the cluster peer liveness snapshot instead of legacy `LivePeers()` when snapshot data is available.

**Architecture:** Add a pure `internal/cluster` Module that evaluates remove-peer membership preflight from target ID, remote voters, and `PeerLivenessRow` snapshot rows. Keep HTTP parsing, force override, membership Adapter invocation, and JSON mapping in `internal/server`. Retain the existing `LivePeers()` path only as a compatibility fallback for cluster implementations without `PeerSnapshot()`.

**Tech Stack:** Go, `testing`, `stretchr/testify`, existing `internal/cluster` snapshot predicates, existing Hertz server handler tests.

---

## File Structure

- Create `internal/cluster/remove_peer_preflight.go`
  - Owns the pure preflight Interface and policy result types.
  - Uses `IsAliveForMembershipMutation` and `BlocksMembershipMutation`.
  - Does not import `internal/server` or perform network/storage I/O.

- Create `internal/cluster/remove_peer_preflight_test.go`
  - Covers configured-as-unknown, live/self counting, down states, unresolved legacy blocking, unresolved legacy target removal, target-not-in-cluster, and quorum math.

- Modify `internal/server/handlers.go`
  - In `handleClusterRemovePeer`, use the new cluster preflight when `s.cluster` implements `PeerSnapshot() []cluster.PeerLivenessRow`.
  - Preserve the legacy `LivePeers()` quorum fallback when no snapshot Interface exists.
  - Map structural not-in-cluster to `404`, quorum failures to the existing `409` shape, and unresolved legacy blockers to `409`.

- Modify `internal/server/cluster_remove_peer_test.go`
  - Add handler-level tests for snapshot preflight JSON behavior, force override, and force not overriding target-not-in-cluster.

## Task 1: Cluster Preflight Module

**Files:**
- Create: `internal/cluster/remove_peer_preflight.go`
- Create: `internal/cluster/remove_peer_preflight_test.go`

- [ ] **Step 1: Write failing cluster policy tests**

Create `internal/cluster/remove_peer_preflight_test.go` with:

```go
package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEvaluateRemovePeerPreflight_ConfiguredDoesNotCountAsAlive(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n3",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessConfigured},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightQuorumWouldBreak, result.Reason)
	require.Equal(t, 2, result.VotersAfter)
	require.Equal(t, 1, result.AliveAfter)
	require.Equal(t, 2, result.NewQuorum)
}

func TestEvaluateRemovePeerPreflight_LivePeersAndSelfSatisfyQuorum(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n3",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessConfigured},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.True(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightAllowed, result.Reason)
	require.Equal(t, 2, result.VotersAfter)
	require.Equal(t, 2, result.AliveAfter)
	require.Equal(t, 2, result.NewQuorum)
}

func TestEvaluateRemovePeerPreflight_ExplicitDownDoesNotCountAsAlive(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n3",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessProbeFailed},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightQuorumWouldBreak, result.Reason)
	require.Equal(t, 1, result.AliveAfter)
}

func TestEvaluateRemovePeerPreflight_UnresolvedLegacyBlocksOtherRemoval(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n2",
		Voters:   []string{"n2", "10.0.0.9:7001"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: PeerIdentityUnresolvedLegacy, LivenessState: PeerLivenessConfigured},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightIdentityUnresolved, result.Reason)
	require.Equal(t, []string{"10.0.0.9:7001"}, result.BlockingPeers)
}

func TestEvaluateRemovePeerPreflight_AllowsRemovingUnresolvedLegacyTarget(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "10.0.0.9:7001",
		Voters:   []string{"n2", "10.0.0.9:7001"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: PeerIdentityUnresolvedLegacy, LivenessState: PeerLivenessConfigured},
		},
	})

	require.True(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightAllowed, result.Reason)
	require.Empty(t, result.BlockingPeers)
	require.Equal(t, 2, result.AliveAfter)
}

func TestEvaluateRemovePeerPreflight_TargetNotInCluster(t *testing.T) {
	result := EvaluateRemovePeerPreflight(RemovePeerPreflightInput{
		TargetID: "n9",
		Voters:   []string{"n2", "n3"},
		Snapshot: []PeerLivenessRow{
			{PeerID: "n1", IdentityState: PeerIdentitySelf, LivenessState: PeerLivenessLive},
			{PeerID: "n2", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
			{PeerID: "n3", IdentityState: PeerIdentityResolved, LivenessState: PeerLivenessLive},
		},
	})

	require.False(t, result.Allowed)
	require.Equal(t, RemovePeerPreflightNotInCluster, result.Reason)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
go test ./internal/cluster -run 'TestEvaluateRemovePeerPreflight' -count=1
```

Expected: FAIL with undefined symbols such as `EvaluateRemovePeerPreflight`.

- [ ] **Step 3: Implement the pure preflight Module**

Create `internal/cluster/remove_peer_preflight.go`:

```go
package cluster

const (
	RemovePeerPreflightAllowed            = "allowed"
	RemovePeerPreflightNotInCluster       = "not_in_cluster"
	RemovePeerPreflightIdentityUnresolved = "identity_unresolved"
	RemovePeerPreflightQuorumWouldBreak   = "quorum_would_break"
)

type RemovePeerPreflightInput struct {
	TargetID string
	Voters   []string
	Snapshot []PeerLivenessRow
}

type RemovePeerPreflightResult struct {
	Allowed       bool
	Reason        string
	VotersAfter   int
	AliveAfter    int
	NewQuorum     int
	BlockingPeers []string
}

func EvaluateRemovePeerPreflight(input RemovePeerPreflightInput) RemovePeerPreflightResult {
	votersAfter := len(input.Voters)
	newQuorum := votersAfter/2 + 1
	if newQuorum < 1 {
		newQuorum = 1
	}
	result := RemovePeerPreflightResult{
		Reason:      RemovePeerPreflightAllowed,
		VotersAfter: votersAfter,
		NewQuorum:   newQuorum,
	}

	if !removePeerTargetInCluster(input.TargetID, input.Voters, input.Snapshot) {
		result.Reason = RemovePeerPreflightNotInCluster
		return result
	}

	for _, row := range input.Snapshot {
		if BlocksMembershipMutation(row) && row.PeerID != input.TargetID {
			result.BlockingPeers = append(result.BlockingPeers, row.PeerID)
		}
	}
	if len(result.BlockingPeers) > 0 {
		result.Reason = RemovePeerPreflightIdentityUnresolved
		return result
	}

	for _, row := range input.Snapshot {
		if row.PeerID == input.TargetID {
			continue
		}
		if IsAliveForMembershipMutation(row) {
			result.AliveAfter++
		}
	}
	if result.AliveAfter < result.NewQuorum {
		result.Reason = RemovePeerPreflightQuorumWouldBreak
		return result
	}

	result.Allowed = true
	return result
}

func removePeerTargetInCluster(target string, voters []string, snapshot []PeerLivenessRow) bool {
	for _, voter := range voters {
		if voter == target {
			return true
		}
	}
	for _, row := range snapshot {
		if row.PeerID == target && row.IdentityState != PeerIdentitySelf {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run cluster tests**

Run:

```bash
go test ./internal/cluster -run 'TestEvaluateRemovePeerPreflight|TestPeerLivenessPredicates' -count=1
```

Expected: PASS.

- [ ] **Step 5: Commit cluster policy Module**

Run:

```bash
git add internal/cluster/remove_peer_preflight.go internal/cluster/remove_peer_preflight_test.go
git commit -m "feat: add remove-peer preflight policy"
```

## Task 2: Server Handler Integration

**Files:**
- Modify: `internal/server/handlers.go`
- Test: `internal/server/cluster_remove_peer_test.go`

- [ ] **Step 1: Write failing server tests**

Append these tests to `internal/server/cluster_remove_peer_test.go`:

```go
func TestClusterRemovePeer_UsesSnapshotPreflightForConfiguredUnknown(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "n3"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessConfigured},
			{PeerID: "n3", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", false)

	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "quorum would break", body["error"])
	require.Equal(t, float64(2), body["voters_after"])
	require.Equal(t, float64(1), body["alive_after"])
	require.Equal(t, float64(2), body["new_quorum"])
	require.Empty(t, mem.called())
}

func TestClusterRemovePeer_BlocksOnUnresolvedLegacyUnlessTarget(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "10.0.0.9:7001"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n2", false)

	require.Equal(t, http.StatusConflict, status)
	require.Equal(t, "membership identity unresolved", body["error"])
	require.Equal(t, []any{"10.0.0.9:7001"}, body["blocking_peers"])
	require.Empty(t, mem.called())
}

func TestClusterRemovePeer_AllowsUnresolvedLegacyTarget(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "10.0.0.9:7001"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "10.0.0.9:7001", IdentityState: cluster.PeerIdentityUnresolvedLegacy, LivenessState: cluster.PeerLivenessConfigured},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "10.0.0.9:7001", false)

	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "removed", body["status"])
	require.Equal(t, []string{"10.0.0.9:7001"}, mem.called())
}

func TestClusterRemovePeer_ForceBypassesSnapshotSafetyButNotMissingTarget(t *testing.T) {
	mem := &fakeMembership{}
	h := setupRemovePeerServer(t, &fakeClusterInfo{
		nodeID:   "n1",
		state:    "Leader",
		leaderID: "n1",
		peers:    []string{"n2", "n3"},
		snapshot: []cluster.PeerLivenessRow{
			{PeerID: "n1", IdentityState: cluster.PeerIdentitySelf, LivenessState: cluster.PeerLivenessLive},
			{PeerID: "n2", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessConfigured},
			{PeerID: "n3", IdentityState: cluster.PeerIdentityResolved, LivenessState: cluster.PeerLivenessLive},
		},
	}, mem)

	status, body := postRemovePeer(t, h.baseURL, "n3", true)
	require.Equal(t, http.StatusOK, status)
	require.Equal(t, "removed", body["status"])
	require.Equal(t, []string{"n3"}, mem.called())

	status, body = postRemovePeer(t, h.baseURL, "n9", true)
	require.Equal(t, http.StatusNotFound, status)
	require.Equal(t, "peer not in cluster", body["error"])
	require.Equal(t, []string{"n3"}, mem.called())
}
```

- [ ] **Step 2: Run server tests to verify they fail**

Run:

```bash
go test ./internal/server -run 'TestClusterRemovePeer_(UsesSnapshotPreflight|BlocksOnUnresolvedLegacy|AllowsUnresolvedLegacyTarget|ForceBypassesSnapshotSafety)' -count=1
```

Expected: FAIL because the handler still uses legacy `LivePeers()` and does not emit the new unresolved-identity response.

- [ ] **Step 3: Add snapshot preflight helpers to the handler file**

In `internal/server/handlers.go`, near the cluster remove-peer handler helpers, add:

```go
func (s *Server) evaluateRemovePeerPreflight(id string) (cluster.RemovePeerPreflightResult, bool) {
	peerSnapshot, ok := s.cluster.(clusterPeerSnapshot)
	if !ok {
		return cluster.RemovePeerPreflightResult{}, false
	}
	return cluster.EvaluateRemovePeerPreflight(cluster.RemovePeerPreflightInput{
		TargetID: id,
		Voters:   s.cluster.Peers(),
		Snapshot: peerSnapshot.PeerSnapshot(),
	}), true
}

func writeRemovePeerPreflightFailure(c *app.RequestContext, result cluster.RemovePeerPreflightResult, id string) {
	switch result.Reason {
	case cluster.RemovePeerPreflightNotInCluster:
		c.JSON(consts.StatusNotFound, map[string]any{
			"error": "peer not in cluster",
			"id":    id,
		})
	case cluster.RemovePeerPreflightIdentityUnresolved:
		c.JSON(consts.StatusConflict, map[string]any{
			"error":          "membership identity unresolved",
			"blocking_peers": result.BlockingPeers,
			"hint":           "remove the unresolved legacy peer first or restore its node ID mapping",
		})
	default:
		c.JSON(consts.StatusConflict, map[string]any{
			"error":        "quorum would break",
			"voters_after": result.VotersAfter,
			"alive_after":  result.AliveAfter,
			"new_quorum":   result.NewQuorum,
			"hint":         "rerun with force=true to override",
		})
	}
}
```

Make sure `handlers.go` already imports `app`, `consts`, and `cluster`; they are already present in this file.

- [ ] **Step 4: Replace the preflight block in `handleClusterRemovePeer`**

Replace the current `peers := s.cluster.Peers()` membership check and `if !req.Force` quorum block with:

```go
	if result, ok := s.evaluateRemovePeerPreflight(req.ID); ok {
		if !result.Allowed {
			if result.Reason == cluster.RemovePeerPreflightNotInCluster || !req.Force {
				writeRemovePeerPreflightFailure(c, result, req.ID)
				return
			}
		}
	} else {
		peers := s.cluster.Peers()
		inCluster := false
		for _, p := range peers {
			if p == req.ID {
				inCluster = true
				break
			}
		}
		if !inCluster {
			c.JSON(consts.StatusNotFound, map[string]any{
				"error": "peer not in cluster",
				"id":    req.ID,
			})
			return
		}

		if !req.Force {
			votersAfter := len(peers)
			newQuorum := votersAfter/2 + 1
			if newQuorum < 1 {
				newQuorum = 1
			}
			live := s.cluster.LivePeers()
			aliveAfter := 0
			for _, p := range live {
				if p == req.ID {
					continue
				}
				aliveAfter++
			}
			if aliveAfter < newQuorum {
				c.JSON(consts.StatusConflict, map[string]any{
					"error":        "quorum would break",
					"voters_after": votersAfter,
					"alive_after":  aliveAfter,
					"new_quorum":   newQuorum,
					"hint":         "rerun with force=true to override",
				})
				return
			}
		}
	}
```

- [ ] **Step 5: Run server tests**

Run:

```bash
go test ./internal/server -run 'TestClusterRemovePeer|TestClusterStatus_DerivesLegacyPeerFieldsFromSnapshot' -count=1
```

Expected: PASS.

- [ ] **Step 6: Commit server integration**

Run:

```bash
git add internal/server/handlers.go internal/server/cluster_remove_peer_test.go
git commit -m "feat: use snapshot preflight for remove-peer"
```

## Task 3: Final Verification and Documentation Check

**Files:**
- Verify: `docs/superpowers/specs/2026-05-06-remove-peer-membership-preflight-design.md`
- Verify: `internal/cluster/remove_peer_preflight.go`
- Verify: `internal/server/handlers.go`

- [ ] **Step 1: Run focused package tests**

Run:

```bash
go test ./internal/cluster ./internal/server -count=1
```

Expected: PASS.

- [ ] **Step 2: Run project test target**

Run:

```bash
make test
```

Expected: PASS.

- [ ] **Step 3: Run diff whitespace check**

Run:

```bash
git diff --check HEAD~2..HEAD
```

Expected: no output and exit code 0.

- [ ] **Step 4: Confirm spec coverage**

Run:

```bash
rg -n "configured|unresolved legacy|force=true|LivePeers|PeerSnapshot|quorum" \
  docs/superpowers/specs/2026-05-06-remove-peer-membership-preflight-design.md \
  internal/cluster/remove_peer_preflight.go \
  internal/cluster/remove_peer_preflight_test.go \
  internal/server/handlers.go \
  internal/server/cluster_remove_peer_test.go
```

Expected:

- spec mentions configured peers as unknown;
- cluster code uses `IsAliveForMembershipMutation`;
- cluster tests cover unresolved legacy target removal;
- server code uses `PeerSnapshot`;
- legacy `LivePeers()` remains only in the no-snapshot fallback.

- [ ] **Step 5: Commit any verification-only doc adjustment if needed**

If Task 3 Step 4 reveals a mismatch between spec language and implementation names, edit only the spec wording and run:

```bash
git add -f docs/superpowers/specs/2026-05-06-remove-peer-membership-preflight-design.md
git commit -m "docs: align remove-peer preflight design"
```

If there is no mismatch, do not create a commit.

## Self-Review

- Spec coverage: covered by Task 1 policy Module, Task 2 server integration, and Task 3 verification.
- Placeholder scan: no placeholder markers or cross-referenced shortcut steps remain.
- Type consistency: `RemovePeerPreflightInput`, `RemovePeerPreflightResult`, and reason constants are defined in Task 1 before Task 2 uses them.
- Scope check: this plan is limited to remove-peer preflight. It does not implement active metaRaft probing or change cluster status display policy.
