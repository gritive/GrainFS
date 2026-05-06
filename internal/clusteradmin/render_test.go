package clusteradmin

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPeersFromStatus_TagsLeaderAndDown(t *testing.T) {
	s := &Status{
		Mode:      "cluster",
		LeaderID:  "n1",
		Peers:     []string{"n3", "n1", "n2"}, // unsorted on the wire
		DownNodes: []string{"n3"},
		PeerAddrs: map[string]string{"n1": "10.0.0.1:7001"},
	}
	rows := PeersFromStatus(s)
	require.Len(t, rows, 3)
	// Sorted output for stable rendering.
	assert.Equal(t, "n1", rows[0].ID)
	assert.Equal(t, "10.0.0.1:7001", rows[0].RaftAddr)
	assert.Equal(t, "leader", rows[0].Role)
	assert.Equal(t, "configured", rows[0].State)
	assert.Equal(t, "n3", rows[2].ID)
	assert.Equal(t, "follower", rows[2].Role)
	assert.Equal(t, "down", rows[2].State)
}

func TestPeersFromStatus_UsesExplicitPeerState(t *testing.T) {
	s := &Status{
		Mode:       "cluster",
		Peers:      []string{"10.0.0.9:7001"},
		PeerStates: map[string]string{"10.0.0.9:7001": "unresolved_legacy"},
	}

	rows := PeersFromStatus(s)
	require.Len(t, rows, 1)
	assert.Equal(t, "unresolved_legacy", rows[0].State)
}

func TestRenderPeersTable(t *testing.T) {
	rows := []PeerRow{
		{ID: "n1", RaftAddr: "10.0.0.1:7001", Role: "leader", State: "configured"},
		{ID: "n3", RaftAddr: "10.0.0.3:7001", Role: "follower", State: "down"},
	}
	var buf bytes.Buffer
	require.NoError(t, RenderPeersTable(&buf, rows))
	out := buf.String()
	assert.Contains(t, out, "NODE_ID")
	assert.Contains(t, out, "RAFT_ADDR")
	assert.Contains(t, out, "10.0.0.1:7001")
	assert.Contains(t, out, "leader")
	assert.Contains(t, out, "down")
}

func TestFilterEventsByAction(t *testing.T) {
	events := []Event{
		{Action: "cluster-join"},
		{Action: "cluster-remove-peer"},
		{Action: "create-bucket"},
	}
	got := FilterEventsByAction(events, []string{"cluster-remove-peer"})
	require.Len(t, got, 1)
	assert.Equal(t, "cluster-remove-peer", got[0].Action)

	// empty filter returns input as-is
	all := FilterEventsByAction(events, nil)
	assert.Equal(t, events, all)
}

func TestEventDetail_RendersMetadataDeterministically(t *testing.T) {
	e := Event{
		Action: "cluster-remove-peer",
		Metadata: map[string]any{
			"removed_id": "n3",
			"force":      false,
		},
	}
	d := EventDetail(e)
	// metadata keys sorted alphabetically: force, removed_id
	idxForce := strings.Index(d, "force=")
	idxRemoved := strings.Index(d, "removed_id=")
	require.GreaterOrEqual(t, idxForce, 0)
	require.GreaterOrEqual(t, idxRemoved, 0)
	assert.Less(t, idxForce, idxRemoved, "metadata keys must render in sorted order")
}
