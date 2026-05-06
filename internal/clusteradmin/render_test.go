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
	}
	rows := PeersFromStatus(s)
	require.Len(t, rows, 3)
	// Sorted output for stable rendering.
	assert.Equal(t, "n1", rows[0].ID)
	assert.Equal(t, "leader", rows[0].Role)
	assert.Equal(t, "alive", rows[0].State)
	assert.Equal(t, "n3", rows[2].ID)
	assert.Equal(t, "follower", rows[2].Role)
	assert.Equal(t, "down", rows[2].State)
}

func TestRenderPeersTable(t *testing.T) {
	rows := []PeerRow{
		{ID: "n1", Role: "leader", State: "alive"},
		{ID: "n3", Role: "follower", State: "down"},
	}
	var buf bytes.Buffer
	require.NoError(t, RenderPeersTable(&buf, rows))
	out := buf.String()
	assert.Contains(t, out, "ID")
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
