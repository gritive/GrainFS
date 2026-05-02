package main

import "testing"

func TestShouldCreateDefaultBucketOnStartup(t *testing.T) {
	tests := []struct {
		name             string
		peers            []string
		recoveryReadOnly bool
		want             bool
	}{
		{name: "single node creates default bucket", want: true},
		{name: "single node recovery read only skips default bucket", recoveryReadOnly: true, want: false},
		{name: "cluster node does not create default bucket per-node", peers: []string{"node-1"}, want: false},
		{name: "cluster recovery read only skips default bucket", peers: []string{"node-1"}, recoveryReadOnly: true, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldCreateDefaultBucketOnStartup(tt.peers, tt.recoveryReadOnly); got != tt.want {
				t.Fatalf("shouldCreateDefaultBucketOnStartup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMetaProposalTargetsPreferLeaderAndFallbackToPeers(t *testing.T) {
	got := metaProposalTargets("node-2", []string{"node-1", "node-2", "node-3"})
	want := []string{"node-2", "node-1", "node-3"}
	if len(got) != len(want) {
		t.Fatalf("metaProposalTargets() len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("metaProposalTargets()[%d] = %q, want %q (%v)", i, got[i], want[i], got)
		}
	}
}

func TestMetaProposalTargetsUsePeersWhenLeaderUnknown(t *testing.T) {
	peers := []string{"node-1", "node-2"}
	got := metaProposalTargets("", peers)
	if len(got) != len(peers) {
		t.Fatalf("metaProposalTargets() len = %d, want %d (%v)", len(got), len(peers), got)
	}
	for i := range peers {
		if got[i] != peers[i] {
			t.Fatalf("metaProposalTargets()[%d] = %q, want %q (%v)", i, got[i], peers[i], got)
		}
	}
}
