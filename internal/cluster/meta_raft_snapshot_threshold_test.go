package cluster

import "testing"

func TestShouldCreateMetaSnapshotUsesLogThreshold(t *testing.T) {
	tests := []struct {
		name              string
		isLeader          bool
		index             uint64
		lastSnapshotIndex uint64
		want              bool
	}{
		{name: "follower", isLeader: false, index: metaSnapshotLogThreshold, want: false},
		{name: "zero-index", isLeader: true, index: 0, want: false},
		{name: "below-threshold", isLeader: true, index: metaSnapshotLogThreshold - 1, want: false},
		{name: "stale-index", isLeader: true, index: metaSnapshotLogThreshold, lastSnapshotIndex: 2 * metaSnapshotLogThreshold, want: false},
		{name: "at-threshold", isLeader: true, index: metaSnapshotLogThreshold, want: true},
		{name: "relative-threshold", isLeader: true, index: 2 * metaSnapshotLogThreshold, lastSnapshotIndex: metaSnapshotLogThreshold, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldCreateMetaSnapshot(tt.isLeader, tt.index, tt.lastSnapshotIndex); got != tt.want {
				t.Fatalf("shouldCreateMetaSnapshot() = %v, want %v", got, tt.want)
			}
		})
	}
}
