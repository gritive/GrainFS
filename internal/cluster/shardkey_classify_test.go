package cluster

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClassifyStartupRepairShardKey(t *testing.T) {
	tests := []struct {
		name     string
		shardKey string
		wantKey  string
		wantKind StartupRepairShardKind
		wantID   string
	}{
		{
			name:     "object-version simple",
			shardKey: "obj/v1",
			wantKey:  "obj",
			wantKind: ShardKindObjectVersion,
			wantID:   "v1",
		},
		{
			name:     "object-version bare (no slash)",
			shardKey: "obj",
			wantKey:  "obj",
			wantKind: ShardKindObjectVersion,
			wantID:   "",
		},
		{
			name:     "empty string",
			shardKey: "",
			wantKey:  "",
			wantKind: ShardKindObjectVersion,
			wantID:   "",
		},
		{
			name:     "segment",
			shardKey: "obj/segments/blob123",
			wantKey:  "obj",
			wantKind: ShardKindSegment,
			wantID:   "blob123",
		},
		{
			name:     "coalesced",
			shardKey: "obj/coalesced/c123",
			wantKey:  "obj",
			wantKind: ShardKindCoalesced,
			wantID:   "c123",
		},
		{
			name:     "nested object key (object-version)",
			shardKey: "a/b/c/v1",
			wantKey:  "a/b/c",
			wantKind: ShardKindObjectVersion,
			wantID:   "v1",
		},
		{
			name:     "marker collision segment with trailing slash in id",
			shardKey: "foo/segments/bar/v1",
			wantKey:  "foo",
			wantKind: ShardKindSegment,
			wantID:   "bar/v1",
		},
		{
			name:     "marker collision coalesced with trailing slash in id",
			shardKey: "a/coalesced/b/v2",
			wantKey:  "a",
			wantKind: ShardKindCoalesced,
			wantID:   "b/v2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotKey, gotKind, gotID := ClassifyStartupRepairShardKey(tt.shardKey)
			require.Equal(t, tt.wantKey, gotKey, "objectKey")
			require.Equal(t, tt.wantKind, gotKind, "kind")
			require.Equal(t, tt.wantID, gotID, "id")
		})
	}
}
