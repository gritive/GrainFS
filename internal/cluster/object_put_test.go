package cluster

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestResolveShardPlacement covers the multi-node streaming PUT's per-shard
// destination mapping: local shards (node == selfID) get "", remote shards get
// the resolved peer address, and a resolution error is surfaced (not silently
// dropped — a dropped remote shard would later read as a missing shard).
func TestResolveShardPlacement(t *testing.T) {
	const self = "node-self"
	resolve := func(node string) (string, error) {
		switch node {
		case "node-b":
			return "10.0.0.2:7000", nil
		case "node-c":
			return "10.0.0.3:7000", nil
		default:
			return "", errors.New("unknown node " + node)
		}
	}

	t.Run("all local", func(t *testing.T) {
		got, err := resolveShardPlacement([]string{self, self, self, self}, self, resolve)
		require.NoError(t, err)
		require.Equal(t, []string{"", "", "", ""}, got)
	})

	t.Run("mixed local and remote", func(t *testing.T) {
		got, err := resolveShardPlacement([]string{self, self, "node-b", "node-c"}, self, resolve)
		require.NoError(t, err)
		require.Equal(t, []string{"", "", "10.0.0.2:7000", "10.0.0.3:7000"}, got)
	})

	t.Run("unresolvable remote node errors", func(t *testing.T) {
		_, err := resolveShardPlacement([]string{self, "node-x"}, self, resolve)
		require.Error(t, err)
		require.Contains(t, err.Error(), "shard 1")
		require.Contains(t, err.Error(), "node-x")
	})
}
