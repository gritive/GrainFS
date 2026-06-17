package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/scrubber"
)

// TestNeedsRedundancyUpgrade pins the detection predicate: an object is a
// relocation candidate iff it is a non-redundant (1+0, parity 0) EC object, the
// cluster currently has redundant placement capacity, it is not a delete marker,
// and it is older than the age gate (avoids racing an in-flight write).
func TestNeedsRedundancyUpgrade(t *testing.T) {
	const now, minAge = int64(1000), int64(60)
	rec := func(data, parity int, deleteMarker bool, lastMod int64) scrubber.ObjectRecord {
		return scrubber.ObjectRecord{
			DataShards: data, ParityShards: parity,
			IsDeleteMarker: deleteMarker, LastModified: lastMod,
		}
	}
	tests := []struct {
		name             string
		rec              scrubber.ObjectRecord
		clusterRedundant bool
		want             bool
	}{
		{"1+0 aged, cluster redundant -> upgrade", rec(1, 0, false, 900), true, true},
		{"1+0 but cluster not redundant -> no", rec(1, 0, false, 900), false, false},
		{"2+2 already redundant -> no", rec(2, 2, false, 900), true, false},
		{"1+1 already redundant -> no", rec(1, 1, false, 900), true, false},
		{"delete marker -> no", rec(1, 0, true, 900), true, false},
		{"too fresh (within age gate) -> no", rec(1, 0, false, 980), true, false},
		{"exactly at age gate boundary -> upgrade", rec(1, 0, false, 940), true, true},
		{"non-EC (data 0) -> no", rec(0, 0, false, 900), true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, needsRedundancyUpgrade(tc.rec, tc.clusterRedundant, now, minAge))
		})
	}
}

// TestClusterHasRedundantCapacity pins the cluster-capacity probe: redundant
// capacity exists iff there are >=2 member nodes AND the widest candidate
// placement group is itself redundant (parity > 0). A genuine single-node
// cluster (or a multi-node cluster whose wide groups have not formed) has none.
func TestClusterHasRedundantCapacity(t *testing.T) {
	cfg := ECConfig{DataShards: 2, ParityShards: 2}
	grp := func(id string, peers int) ShardGroupEntry {
		p := make([]string, peers)
		for i := range p {
			p[i] = string(rune('a' + i))
		}
		return ShardGroupEntry{ID: id, PeerIDs: p}
	}
	tests := []struct {
		name          string
		groups        []ShardGroupEntry
		metaNodeCount int
		want          bool
	}{
		{"4-node wide group -> redundant", []ShardGroupEntry{grp("group-12", 4)}, 4, true},
		{"3-node 3-peer group -> redundant", []ShardGroupEntry{grp("group-9", 3)}, 3, true},
		{"2-node 2-peer group -> redundant", []ShardGroupEntry{grp("group-8", 2)}, 2, true},
		{"2-node only 1-peer groups -> none (forming)", []ShardGroupEntry{grp("group-5", 1)}, 2, false},
		{"1-node 1-peer group -> none (single node)", []ShardGroupEntry{grp("group-5", 1)}, 1, false},
		{"no groups -> none", nil, 4, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, clusterHasRedundantCapacity(tc.groups, cfg, tc.metaNodeCount))
		})
	}
}
