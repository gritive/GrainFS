package cluster_test

import (
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSplitBrainDetector_NoSplitBrain(t *testing.T) {
	// One leader + one follower → no split brain
	peers := []cluster.PeerStatus{
		{NodeID: "node-1", State: "Leader", Term: 5},
		{NodeID: "node-2", State: "Follower", Term: 5},
		{NodeID: "node-3", State: "Follower", Term: 5},
	}
	d := cluster.NewSplitBrainDetector()
	suspected := d.Detect(peers)
	assert.False(t, suspected, "single leader → no split brain")
}

func TestSplitBrainDetector_MultipleLeaders(t *testing.T) {
	// Two nodes both claim to be Leader → split brain
	peers := []cluster.PeerStatus{
		{NodeID: "node-1", State: "Leader", Term: 5},
		{NodeID: "node-2", State: "Leader", Term: 4},
		{NodeID: "node-3", State: "Follower", Term: 5},
	}
	d := cluster.NewSplitBrainDetector()
	suspected := d.Detect(peers)
	assert.True(t, suspected, "two leaders → split brain suspected")
}

func TestSplitBrainDetector_TermDivergence(t *testing.T) {
	// All followers but term diverged by >1 → possible network partition
	peers := []cluster.PeerStatus{
		{NodeID: "node-1", State: "Follower", Term: 10},
		{NodeID: "node-2", State: "Follower", Term: 3},
	}
	d := cluster.NewSplitBrainDetector()
	suspected := d.Detect(peers)
	assert.True(t, suspected, "large term divergence → split brain suspected")
}

func TestSplitBrainDetector_NoPeers(t *testing.T) {
	d := cluster.NewSplitBrainDetector()
	suspected := d.Detect(nil)
	assert.False(t, suspected, "no peers → no split brain")
}

func TestSplitBrainDetector_SingleNode(t *testing.T) {
	peers := []cluster.PeerStatus{
		{NodeID: "node-1", State: "Leader", Term: 1},
	}
	d := cluster.NewSplitBrainDetector()
	suspected := d.Detect(peers)
	assert.False(t, suspected, "single node → no split brain")
}

func TestNewSplitBrainDetector(t *testing.T) {
	d := cluster.NewSplitBrainDetector()
	require.NotNil(t, d)
}
