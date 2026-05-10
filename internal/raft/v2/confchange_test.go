package raftv2

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestConfChange_RoundtripSingle verifies that a single-config payload
// (Cnew only) survives encode + decode unchanged.
func TestConfChange_RoundtripSingle(t *testing.T) {
	cases := []struct {
		name   string
		voters []string
	}{
		{name: "empty", voters: nil},
		{name: "one", voters: []string{"n1"}},
		{name: "three", voters: []string{"n1", "n2", "n3"}},
		{name: "long-id", voters: []string{"node-aaaa-bbbb-cccc"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			blob := encodeConfChange(tc.voters)
			got, err := decodeConfChange(blob)
			require.NoError(t, err)
			require.False(t, got.IsJoint)
			if len(tc.voters) == 0 {
				require.Empty(t, got.NewVoters)
			} else {
				require.True(t, reflect.DeepEqual(tc.voters, got.NewVoters),
					"NewVoters mismatch: %v vs %v", tc.voters, got.NewVoters)
			}
			require.Empty(t, got.OldVoters)
		})
	}
}

// TestConfChange_RoundtripJoint verifies a joint-config payload survives
// encode + decode with both Cold and Cnew preserved in order.
func TestConfChange_RoundtripJoint(t *testing.T) {
	old := []string{"n1", "n2", "n3"}
	newV := []string{"n1", "n2", "n3", "n4", "n5"}
	blob := encodeJointConfChange(old, newV)
	got, err := decodeConfChange(blob)
	require.NoError(t, err)
	require.True(t, got.IsJoint)
	require.True(t, reflect.DeepEqual(newV, got.NewVoters))
	require.True(t, reflect.DeepEqual(old, got.OldVoters))
}

// TestConfChange_DecodeRejectsTruncated verifies that decode surfaces a
// clear error rather than panicking on a short / corrupt blob.
func TestConfChange_DecodeRejectsTruncated(t *testing.T) {
	full := encodeJointConfChange([]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4"})
	for trunc := 0; trunc < len(full); trunc++ {
		_, err := decodeConfChange(full[:trunc])
		require.Error(t, err, "truncated to %d bytes must error", trunc)
	}
}

// TestConfChange_DecodeRejectsUnknownVersion ensures an unknown version byte
// is refused so future encodings can break old readers loudly.
func TestConfChange_DecodeRejectsUnknownVersion(t *testing.T) {
	blob := encodeConfChange([]string{"n1"})
	blob[0] = 0xFF
	_, err := decodeConfChange(blob)
	require.Error(t, err)
}

// TestConfChange_DecodeRejectsUnknownKind ensures an unknown kind byte is
// refused (defends against future kinds being silently treated as single).
func TestConfChange_DecodeRejectsUnknownKind(t *testing.T) {
	blob := encodeConfChange([]string{"n1"})
	blob[1] = 0x7F
	_, err := decodeConfChange(blob)
	require.Error(t, err)
}

// TestEffectiveConfig_AllVotersSingle: in non-joint state allVoters returns
// exactly the voters slice (copy).
func TestEffectiveConfig_AllVotersSingle(t *testing.T) {
	c := newSingleConfig([]string{"n1", "n2", "n3"})
	got := c.allVoters()
	require.Equal(t, []string{"n1", "n2", "n3"}, got)
	// Mutating the returned slice must not affect the config.
	got[0] = "X"
	require.Equal(t, []string{"n1", "n2", "n3"}, c.voters)
}

// TestEffectiveConfig_AllVotersJointDeduplicated: joint state unions Cold and
// Cnew without duplicates and preserves Cnew-first order.
func TestEffectiveConfig_AllVotersJointDeduplicated(t *testing.T) {
	c := newJointConfig(
		[]string{"n1", "n2", "n3"},       // Cold
		[]string{"n1", "n2", "n4", "n5"}, // Cnew (drops n3, adds n4, n5)
	)
	got := c.allVoters()
	// Cnew first (in encoded order), then Cold-only additions.
	require.Equal(t, []string{"n1", "n2", "n4", "n5", "n3"}, got)
}

// TestEffectiveConfig_PeersExcluding drops self while preserving order.
func TestEffectiveConfig_PeersExcluding(t *testing.T) {
	c := newSingleConfig([]string{"n1", "n2", "n3"})
	require.Equal(t, []string{"n2", "n3"}, c.peersExcluding("n1"))
	require.Equal(t, []string{"n1", "n3"}, c.peersExcluding("n2"))
}

// TestEffectiveConfig_QuorumSingle covers majority-of-one-set quorum logic.
func TestEffectiveConfig_QuorumSingle(t *testing.T) {
	c := newSingleConfig([]string{"n1", "n2", "n3"})
	require.False(t, c.quorumOK(map[string]bool{"n1": true}))            // 1/3
	require.True(t, c.quorumOK(map[string]bool{"n1": true, "n2": true})) // 2/3
}

// TestEffectiveConfig_QuorumJointRequiresBoth: joint quorum needs majority
// from BOTH Cold and Cnew, independently.
func TestEffectiveConfig_QuorumJointRequiresBoth(t *testing.T) {
	c := newJointConfig(
		[]string{"a", "b", "c"},           // Cold (3 voters; majority = 2)
		[]string{"c", "d", "e", "f", "g"}, // Cnew (5 voters; majority = 3)
	)

	// Majority of Cold (a+b) but not of Cnew → fail.
	require.False(t, c.quorumOK(map[string]bool{"a": true, "b": true}))

	// Majority of Cnew (d+e+f) but not of Cold → fail (only c shared, 1/3).
	require.False(t, c.quorumOK(map[string]bool{"d": true, "e": true, "f": true}))

	// Majority of both: a+b from Cold (2/3) AND c+d+e from Cnew (3/5).
	require.True(t, c.quorumOK(map[string]bool{
		"a": true, "b": true, "c": true, "d": true, "e": true,
	}))
}

// TestEffectiveConfig_ContainsVoter checks Cnew-side membership lookup.
func TestEffectiveConfig_ContainsVoter(t *testing.T) {
	c := newJointConfig([]string{"a", "b"}, []string{"b", "c"})
	require.True(t, c.containsVoter("b"))
	require.True(t, c.containsVoter("c"))
	require.False(t, c.containsVoter("a"), "a is in Cold but not Cnew")
}

// TestApplyConfigEntry_Single transitions an effective config from any prior
// state into a fresh single state (Cnew alone).
func TestApplyConfigEntry_Single(t *testing.T) {
	prev := newJointConfig([]string{"n1", "n2"}, []string{"n1", "n2", "n3"})
	e := LogEntry{
		Type:    LogEntryConfChange,
		Index:   42,
		Term:    7,
		Command: encodeConfChange([]string{"n1", "n2", "n3"}),
	}
	got := applyConfigEntry(prev, e)
	require.False(t, got.joint)
	require.Equal(t, []string{"n1", "n2", "n3"}, got.voters)
}

// TestApplyConfigEntry_Joint transitions into the joint state.
func TestApplyConfigEntry_Joint(t *testing.T) {
	prev := newSingleConfig([]string{"n1", "n2", "n3"})
	e := LogEntry{
		Type:    LogEntryJointConfChange,
		Index:   10,
		Term:    3,
		Command: encodeJointConfChange([]string{"n1", "n2", "n3"}, []string{"n1", "n2", "n3", "n4"}),
	}
	got := applyConfigEntry(prev, e)
	require.True(t, got.joint)
	require.Equal(t, []string{"n1", "n2", "n3", "n4"}, got.voters)
	require.Equal(t, []string{"n1", "n2", "n3"}, got.oldVoters)
}
