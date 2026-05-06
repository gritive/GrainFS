package clusteradmin

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPreflightCheck verifies the quorum math for the canonical Day-2
// scenarios. Convention: peers excludes the running node (self), live
// includes it.
func TestPreflightCheck(t *testing.T) {
	cases := []struct {
		name      string
		peers     []string // remote voters
		live      []string // includes selfID
		target    string
		wantBlock bool
		wantAlive int
		wantQ     int
	}{
		{
			name:      "remove dead peer in healthy 3-cluster",
			peers:     []string{"n2", "n3"},
			live:      []string{"n1", "n2"}, // self + n2; n3 dead
			target:    "n3",
			wantBlock: false, wantAlive: 2, wantQ: 2,
		},
		{
			name:      "remove alive peer in 3-cluster with one already dead — blocks",
			peers:     []string{"n2", "n3"},
			live:      []string{"n1", "n2"}, // n3 already dead
			target:    "n2",
			wantBlock: true, wantAlive: 1, wantQ: 2,
		},
		{
			name:      "remove healthy peer in fully healthy 3-cluster",
			peers:     []string{"n2", "n3"},
			live:      []string{"n1", "n2", "n3"},
			target:    "n3",
			wantBlock: false, wantAlive: 2, wantQ: 2,
		},
		{
			name:      "5-cluster with 3 alive — remove dead",
			peers:     []string{"n2", "n3", "n4", "n5"},
			live:      []string{"n1", "n2", "n3"}, // n4, n5 dead
			target:    "n5",
			wantBlock: false, wantAlive: 3, wantQ: 3,
		},
		{
			name:      "5-cluster with 2 alive — remove blocks",
			peers:     []string{"n2", "n3", "n4", "n5"},
			live:      []string{"n1", "n2"}, // 3, 4, 5 dead
			target:    "n3",
			wantBlock: true, wantAlive: 2, wantQ: 3,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := Check(tc.peers, tc.live, tc.target)
			assert.Equal(t, tc.wantBlock, p.WouldBlock, "WouldBlock")
			assert.Equal(t, tc.wantAlive, p.AliveAfter, "AliveAfter")
			assert.Equal(t, tc.wantQ, p.NewQuorum, "NewQuorum")
		})
	}
}

func TestLivePeersFromStatus(t *testing.T) {
	// peers excludes self ("n1"); n3 is in down_nodes.
	live := LivePeersFromStatus([]string{"n2", "n3"}, []string{"n3"}, "n1")
	assert.Equal(t, []string{"n1", "n2"}, live, "self always alive, downed peers filtered")

	// no down_nodes: self prepended, all peers kept.
	all := LivePeersFromStatus([]string{"n2"}, nil, "n1")
	assert.Equal(t, []string{"n1", "n2"}, all)

	// missing selfID still works (caller in local mode without cluster).
	noSelf := LivePeersFromStatus([]string{"n2"}, nil, "")
	assert.Equal(t, []string{"n2"}, noSelf)
}

func TestContains(t *testing.T) {
	assert.True(t, Contains([]string{"a", "b"}, "a"))
	assert.False(t, Contains([]string{"a", "b"}, "c"))
	assert.False(t, Contains(nil, "a"))
}
