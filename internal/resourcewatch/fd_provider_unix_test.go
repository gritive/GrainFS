//go:build unix

package resourcewatch

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessFDProvider_Snapshot(t *testing.T) {
	f, err := os.Open("fd_provider_unix_test.go")
	require.NoError(t, err)
	defer f.Close()

	snapshot, err := NewFDProvider(FDProviderOptions{}).Snapshot(context.Background())
	require.NoError(t, err)
	assert.Positive(t, snapshot.Open)
	assert.Positive(t, snapshot.Limit)
	assert.False(t, snapshot.CollectedAt.IsZero())
	assert.NotNil(t, snapshot.Categories)
}

func TestClassifyFDTarget(t *testing.T) {
	tests := []struct {
		name   string
		target string
		want   FDCategory
	}{
		{name: "socket", target: "socket:[12345]", want: FDCategorySocket},
		{name: "badger", target: "/tmp/grainfs/000001.sst", want: FDCategoryBadger},
		{name: "receipt store", target: "/tmp/grainfs/receipt-store.log", want: FDCategoryReceiptOrEventStore},
		{name: "nfs session", target: "/tmp/grainfs/nfs-session.db", want: FDCategoryNFSSession},
		{name: "regular file", target: "/tmp/grainfs/data.bin", want: FDCategoryRegularFile},
		{name: "unknown", target: "pipe:[12345]", want: FDCategoryUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, classifyFDTarget(tt.target))
		})
	}
}
