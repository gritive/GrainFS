package serveruntime

import (
	"reflect"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
)

func TestSeedShardGroupPeerIDs_SelfFirstAddressBookFallback(t *testing.T) {
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-b", Address: "10.0.0.2:7000"},
		{ID: "node-c", Address: "10.0.0.3:7000"},
	}
	got := seedShardGroupPeerIDs("node-a", "10.0.0.1:7000", []string{"10.0.0.2:7000", "10.0.0.3:7000"}, nodes)
	want := []string{"node-a", "node-b", "node-c"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestSeedShardGroupPeerIDs_UnknownPeerKeepsAddress(t *testing.T) {
	got := seedShardGroupPeerIDs("node-a", "10.0.0.1:7000", []string{"10.0.0.99:7000"}, nil)
	want := []string{"node-a", "10.0.0.99:7000"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestSeedShardGroupPeerIDs_EmptySelfNodeIDFallsBackToAddr(t *testing.T) {
	got := seedShardGroupPeerIDs("", "10.0.0.1:7000", nil, nil)
	want := []string{"10.0.0.1:7000"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v want %v", got, want)
	}
}

func TestSeedShardGroupVoters_Group0UsesAllPeers(t *testing.T) {
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-b", Address: "10.0.0.2:7000"},
	}
	got := SeedShardGroupVoters("node-a", "10.0.0.1:7000", []string{"10.0.0.2:7000"}, nodes, "group-0", 3)
	want := []string{"node-a", "node-b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("group-0 should keep full peer set: got %v want %v", got, want)
	}
}

func TestSeedShardGroupVoters_NonZeroGroupUsesEffectiveECWidth(t *testing.T) {
	nodes := []cluster.MetaNodeEntry{
		{ID: "node-b", Address: "10.0.0.2:7000"},
		{ID: "node-c", Address: "10.0.0.3:7000"},
		{ID: "node-d", Address: "10.0.0.4:7000"},
	}
	peers := []string{"10.0.0.2:7000", "10.0.0.3:7000", "10.0.0.4:7000"}
	got := SeedShardGroupVoters("node-a", "10.0.0.1:7000", peers, nodes, "group-7", 4)
	if len(got) != 4 {
		t.Fatalf("want 4 voters, got %d (%v)", len(got), got)
	}
	wantSet := map[string]bool{"node-a": true, "node-b": true, "node-c": true, "node-d": true}
	for _, voter := range got {
		if !wantSet[voter] {
			t.Fatalf("unexpected voter %q in %v", voter, got)
		}
	}
	// PickVoters is deterministic given the same group id + peer set, so
	// re-running should yield identical output.
	got2 := SeedShardGroupVoters("node-a", "10.0.0.1:7000", peers, nodes, "group-7", 4)
	if !reflect.DeepEqual(got, got2) {
		t.Fatalf("PickVoters not deterministic: %v vs %v", got, got2)
	}
}
