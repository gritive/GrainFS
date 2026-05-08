package serveruntime

import (
	"reflect"
	"testing"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/server/admin"
	"github.com/gritive/GrainFS/internal/volume"
)

func TestAggregateVolumeReplicaLayout(t *testing.T) {
	// Helper: build a 4+2 EC entry on a placement group that has 6 voters,
	// landing on the first len(NodeIDs) of those voters.
	mkEntry := func(volName, blockSuffix, groupID string, ecData, ecParity uint8, nodeIDs []string) cluster.ObjectIndexEntry {
		return cluster.ObjectIndexEntry{
			Bucket:           volume.VolumeBucketName,
			Key:              volume.BlockKeyPrefix(volName) + blockSuffix,
			VersionID:        "v1",
			PlacementGroupID: groupID,
			ECData:           ecData,
			ECParity:         ecParity,
			NodeIDs:          nodeIDs,
		}
	}
	// 6-voter group → desired EC profile is 4+2 per topology_policy zeroConfig table.
	groupSixNodes := cluster.ShardGroupEntry{
		ID:      "g1",
		PeerIDs: []string{"n1", "n2", "n3", "n4", "n5", "n6"},
	}
	groups := map[string]cluster.ShardGroupEntry{"g1": groupSixNodes}

	cases := []struct {
		name    string
		entries []cluster.ObjectIndexEntry
		names   []string
		want    map[string]admin.ReplicaLayoutFact
	}{
		{
			name:    "no entries → nil",
			entries: nil,
			names:   []string{"v1"},
			want:    nil,
		},
		{
			name:    "no names → nil",
			entries: []cluster.ObjectIndexEntry{mkEntry("v1", "000000000001", "g1", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"})},
			names:   nil,
			want:    nil,
		},
		{
			name:    "single current entry — counts CurrentCount",
			entries: []cluster.ObjectIndexEntry{mkEntry("v1", "000000000001", "g1", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"})},
			names:   []string{"v1"},
			want:    map[string]admin.ReplicaLayoutFact{"v1": {CurrentCount: 1}},
		},
		{
			name: "missing nodeIDs entry — counts RepairNeededCount",
			entries: []cluster.ObjectIndexEntry{
				mkEntry("v1", "000000000001", "g1", 4, 2, nil),
			},
			names: []string{"v1"},
			want:  map[string]admin.ReplicaLayoutFact{"v1": {RepairNeededCount: 1}},
		},
		{
			name: "shorter NodeIDs than k+m — counts RepairNeededCount",
			entries: []cluster.ObjectIndexEntry{
				mkEntry("v1", "000000000001", "g1", 4, 2, []string{"n1", "n2", "n3"}),
			},
			names: []string{"v1"},
			want:  map[string]admin.ReplicaLayoutFact{"v1": {RepairNeededCount: 1}},
		},
		{
			name: "missing placement group — counts UnknownCount",
			entries: []cluster.ObjectIndexEntry{
				mkEntry("v1", "000000000001", "ghost", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"}),
			},
			names: []string{"v1"},
			want:  map[string]admin.ReplicaLayoutFact{"v1": {UnknownCount: 1}},
		},
		{
			name: "many states across two volumes",
			entries: []cluster.ObjectIndexEntry{
				mkEntry("v1", "000000000001", "g1", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"}),    // current
				mkEntry("v1", "000000000002", "g1", 4, 2, nil),                                             // repair-needed
				mkEntry("v2", "000000000001", "g1", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"}),    // current
				mkEntry("v2", "000000000002", "g1", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"}),    // current
				mkEntry("v2", "000000000003", "ghost", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"}), // unknown
			},
			names: []string{"v1", "v2"},
			want: map[string]admin.ReplicaLayoutFact{
				"v1": {CurrentCount: 1, RepairNeededCount: 1},
				"v2": {CurrentCount: 2, UnknownCount: 1},
			},
		},
		{
			name: "entry for volume not in names — ignored",
			entries: []cluster.ObjectIndexEntry{
				mkEntry("foreign", "000000000001", "g1", 4, 2, nil),
			},
			names: []string{"v1"},
			want:  nil,
		},
		{
			name: "entry with non-block key — ignored",
			entries: []cluster.ObjectIndexEntry{
				{
					Bucket:           volume.VolumeBucketName,
					Key:              volume.MetaPrefix + "v1/meta",
					PlacementGroupID: "g1",
					ECData:           4,
					ECParity:         2,
					NodeIDs:          []string{"n1", "n2", "n3", "n4", "n5", "n6"},
				},
			},
			names: []string{"v1"},
			want:  nil,
		},
		{
			name:    "empty names list — nil",
			entries: []cluster.ObjectIndexEntry{mkEntry("v1", "000000000001", "g1", 4, 2, []string{"n1", "n2", "n3", "n4", "n5", "n6"})},
			names:   []string{""},
			want:    nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := aggregateVolumeReplicaLayout(tc.entries, groups, tc.names)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
