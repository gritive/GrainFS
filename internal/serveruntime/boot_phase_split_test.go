package serveruntime

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// seqNames returns the ordered phase names of bootSequence(), and indexOf
// returns the position of a phase name (or -1). The ordering invariants below
// assert on these positions — name-based and refactor-proof, replacing the old
// strings.Index(run.go-source, ...) guards that broke silently on any rename or
// reformat of run.go.
func seqNames(t *testing.T) []string {
	t.Helper()
	seq := bootSequence()
	names := make([]string, len(seq))
	for i, p := range seq {
		require.NotEmpty(t, p.name, "phase %d has empty name", i)
		require.NotNil(t, p.run, "phase %q (%d) has nil run", p.name, i)
		names[i] = p.name
	}
	return names
}

func indexOf(names []string, want string) int {
	for i, n := range names {
		if n == want {
			return i
		}
	}
	return -1
}

// TestBootSequenceOrdering_R1Narrow ports the R1-narrow ordering invariants
// from the former source-grep guard onto bootSequence() positions.
func TestBootSequenceOrdering_R1Narrow(t *testing.T) {
	names := seqNames(t)
	// The keeper is populated (bootWALAndForwardersPart1) before the DEK gate
	// (bootWaitDEKReady).
	assert.Greater(t, indexOf(names, "bootWaitDEKReady"), indexOf(names, "bootWALAndForwardersPart1"))
	// The backend is wrapped after routed storage exists.
	assert.Greater(t, indexOf(names, "bootClusterCoordinatorRouting"), indexOf(names, "bootOwnedGroupsAndEC"))
	assert.Greater(t, indexOf(names, "bootBackendWrap"), indexOf(names, "bootClusterCoordinatorRouting"))
	assert.Greater(t, indexOf(names, "bootBackendWrap"), indexOf(names, "bootSnapshotAndApplyLoop"))
}

// TestBootSequenceOrdering_RFSMAlpha ports the R-FSM-α reorder invariants.
func TestBootSequenceOrdering_RFSMAlpha(t *testing.T) {
	names := seqNames(t)
	// bootShardService (constructs the data encryptor) runs after the DEK gate.
	assert.Greater(t, indexOf(names, "bootShardService"), indexOf(names, "bootWaitDEKReady"))
	// Forward handlers register after bootShardService.
	assert.Greater(t, indexOf(names, "bootRegisterForwardHandlers"), indexOf(names, "bootShardService"))
	// Forward handlers register before bootBalancerAndGossip.
	assert.Greater(t, indexOf(names, "bootBalancerAndGossip"), indexOf(names, "bootRegisterForwardHandlers"))
	// bootWALAndForwardersPart1 runs before the DEK gate.
	assert.Less(t, indexOf(names, "bootWALAndForwardersPart1"), indexOf(names, "bootWaitDEKReady"))
}

// TestBootSequenceOrdering_ExtractedInlineBlocks pins the homes of the three
// formerly-inline blocks (Slice 1) relative to their neighbors.
func TestBootSequenceOrdering_ExtractedInlineBlocks(t *testing.T) {
	names := seqNames(t)
	// bootDataRaftNode (run.go:78-134 verbatim) constructs + early-Starts the
	// data-raft node after the transport phases and before meta-raft wiring —
	// the early Start must precede the invite-join Phase-2 inside
	// bootWALAndForwardersPart1 (5-node EC join deadlock avoidance).
	assert.Greater(t, indexOf(names, "bootDataRaftNode"), indexOf(names, "bootGroupRaftMux"))
	assert.Less(t, indexOf(names, "bootDataRaftNode"), indexOf(names, "bootMetaRaftWiring"))
	assert.Less(t, indexOf(names, "bootDataRaftNode"), indexOf(names, "bootWALAndForwardersPart1"))
	// bootGreenfieldDEKBoundary runs after the scrubber and before services.
	assert.Greater(t, indexOf(names, "bootGreenfieldDEKBoundary"), indexOf(names, "bootRecoveryAndScrubber"))
	assert.Less(t, indexOf(names, "bootGreenfieldDEKBoundary"), indexOf(names, "bootDegradedAndServices"))
	// wireRewrapLanes runs after bootBackendWrap (needs distBackend +
	// packedBackend) and before bootRegisterForwardHandlers.
	assert.Greater(t, indexOf(names, "wireRewrapLanes"), indexOf(names, "bootBackendWrap"))
	assert.Less(t, indexOf(names, "wireRewrapLanes"), indexOf(names, "bootRegisterForwardHandlers"))
	// bootSelfRegisterMember runs last (after Phase-2 membership promotion).
	assert.Equal(t, len(names)-1, indexOf(names, "bootSelfRegisterMember"))
}

// TestBootSequenceCompleteness asserts that bootSequence() is exactly the set
// of boot phases below — no duplicates, no phase defined-but-missing from the
// slice, no stale slice entry. The expected set is hand-maintained (no
// reflection): adding/removing a phase must update both the slice and this set,
// which is the point — the test fails loud on either drift.
//
// bootShutdownDrain is intentionally excluded: it returns no error and runs
// after the loop, not as a sequence phase.
func TestBootSequenceCompleteness(t *testing.T) {
	expected := []string{
		"bootValidateConfig",
		"bootAutoMigrate",
		"bootOpenMetaDB",
		"bootValidateTimings",
		"bootOpenSharedFSMDB",
		"bootRaftStoreKey",
		"bootClusterTransport",
		"bootGroupRaftMux",
		"bootDataRaftNode",
		"bootMetaRaftWiring",
		"bootDataGroupRouter",
		"bootRotationAndAdminAPI",
		"bootKEKRotationLeader",
		"bootMetaRaftStart",
		"bootGenesisDEKBootstrap",
		"bootWALAndForwardersPart1",
		"bootWaitDEKReady",
		"bootShardService",
		"bootShardRoutes",
		"bootOwnedGroupsAndEC",
		"bootClusterCoordinatorRouting",
		"bootSnapshotAndApplyLoop",
		"bootBackendWrap",
		"wireRewrapLanes",
		"bootRegisterForwardHandlers",
		"bootBalancerAndGossip",
		"bootSrvOptsAndReceipt",
		"bootHTTPServerAndAdmin",
		"bootPhase0Banner",
		"bootRecoveryAndScrubber",
		"bootGreenfieldDEKBoundary",
		"bootDegradedAndServices",
		"bootSelfRegisterMember",
	}
	names := seqNames(t)

	// No duplicate appears in the slice.
	seen := map[string]int{}
	for _, n := range names {
		seen[n]++
	}
	for n, c := range seen {
		assert.Equalf(t, 1, c, "phase %q appears %d times in bootSequence (want exactly once)", n, c)
	}

	// Exact set + order equality with the hand-maintained expectation.
	assert.Equal(t, expected, names,
		"bootSequence() drifted from the expected phase set/order; update both the slice and this test")
}

// TestBootSequenceCoversAllDefinedPhases is the source-side half of the
// completeness guard: every bootXxx error-returning phase function DEFINED in
// the package appears in bootSequence() (catches "defined but never wired").
// Phases that are deliberately NOT sequence steps are listed in nonSequence.
func TestBootSequenceCoversAllDefinedPhases(t *testing.T) {
	// Phase funcs that exist but are intentionally not bootSequence() entries.
	nonSequence := map[string]bool{
		"bootSequence":         true, // the registry builder itself, not a phase
		"bootShutdownDrain":    true, // post-loop, returns no error
		"bootInviteJoinPhase2": true, // a sub-phase invoked inside bootWALAndForwardersPart1
	}

	entries, err := os.ReadDir(".")
	require.NoError(t, err)
	defined := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		b, err := os.ReadFile(name)
		require.NoError(t, err)
		for _, line := range strings.Split(string(b), "\n") {
			const pfx = "func boot"
			if !strings.HasPrefix(line, pfx) {
				continue
			}
			// Extract the identifier up to the '(' — skips method receivers
			// (those start with "func (").
			rest := line[len("func "):]
			paren := strings.IndexByte(rest, '(')
			if paren <= 0 {
				continue
			}
			defined[rest[:paren]] = true
		}
	}
	require.NotEmpty(t, defined, "expected to discover bootXxx phase definitions")

	inSeq := map[string]bool{}
	for _, n := range seqNames(t) {
		inSeq[n] = true
	}
	for name := range defined {
		if nonSequence[name] {
			continue
		}
		assert.Truef(t, inSeq[name],
			"phase %q is defined but missing from bootSequence() (add it, or list it in nonSequence)", name)
	}
}

// TestForwardersPhaseDoesNotOpenLogicalWAL is preserved from the former
// source-grep file: it asserts bootWALAndForwardersPart1 no longer opens the
// logical WAL. This is a source check on boot_phases_forwarders.go, independent
// of run.go ordering, so it stays as-is.
func TestForwardersPhaseDoesNotOpenLogicalWAL(t *testing.T) {
	b, err := os.ReadFile("boot_phases_forwarders.go")
	require.NoError(t, err)
	fn := string(b)
	start := strings.Index(fn, "func bootWALAndForwardersPart1(")
	require.GreaterOrEqual(t, start, 0)
	end := strings.Index(fn[start+1:], "\nfunc ") + start + 1
	assert.NotContains(t, fn[start:end], "wal.OpenEncrypted(")
}
