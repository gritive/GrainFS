package receipt

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRoutingCache_Lookup_EmptyCache(t *testing.T) {
	c := NewRoutingCache()

	node, ok := c.Lookup("rcpt-missing")
	require.False(t, ok)
	require.Empty(t, node)
}

func TestRoutingCache_Update_SingleNode(t *testing.T) {
	c := NewRoutingCache()
	c.Update("node-a", []string{"rcpt-1", "rcpt-2", "rcpt-3"})

	for _, id := range []string{"rcpt-1", "rcpt-2", "rcpt-3"} {
		node, ok := c.Lookup(id)
		require.True(t, ok, "should find %s", id)
		require.Equal(t, "node-a", node)
	}
	_, ok := c.Lookup("rcpt-unknown")
	require.False(t, ok)
}

func TestRoutingCache_Update_MultipleNodes(t *testing.T) {
	c := NewRoutingCache()
	c.Update("node-a", []string{"rcpt-a1", "rcpt-a2"})
	c.Update("node-b", []string{"rcpt-b1", "rcpt-b2"})

	tests := []struct {
		id       string
		wantNode string
	}{
		{"rcpt-a1", "node-a"},
		{"rcpt-a2", "node-a"},
		{"rcpt-b1", "node-b"},
		{"rcpt-b2", "node-b"},
	}
	for _, tt := range tests {
		node, ok := c.Lookup(tt.id)
		require.True(t, ok, "lookup %s", tt.id)
		require.Equal(t, tt.wantNode, node)
	}
}

func TestRoutingCache_Update_RollingReplace(t *testing.T) {
	// Rolling window: a second Update from the same node replaces the prior
	// list entirely. IDs from the old list are no longer found.
	c := NewRoutingCache()
	c.Update("node-a", []string{"rcpt-old-1", "rcpt-old-2"})
	c.Update("node-a", []string{"rcpt-new-1", "rcpt-new-2"})

	_, ok := c.Lookup("rcpt-old-1")
	require.False(t, ok, "old IDs must evict on replace")
	_, ok = c.Lookup("rcpt-old-2")
	require.False(t, ok)

	node, ok := c.Lookup("rcpt-new-1")
	require.True(t, ok)
	require.Equal(t, "node-a", node)
}

func TestRoutingCache_Evict_RemovesNode(t *testing.T) {
	c := NewRoutingCache()
	c.Update("node-a", []string{"rcpt-a1"})
	c.Update("node-b", []string{"rcpt-b1"})

	c.Evict("node-a")

	_, ok := c.Lookup("rcpt-a1")
	require.False(t, ok, "evicted node's IDs must not be found")

	node, ok := c.Lookup("rcpt-b1")
	require.True(t, ok, "other nodes unaffected")
	require.Equal(t, "node-b", node)
}

func TestRoutingCache_Evict_UnknownNode(t *testing.T) {
	c := NewRoutingCache()
	c.Update("node-a", []string{"rcpt-a1"})

	// Evicting a node that was never Update'd is a no-op, not an error.
	c.Evict("node-nonexistent")

	_, ok := c.Lookup("rcpt-a1")
	require.True(t, ok)
}

func TestRoutingCache_Update_EmptyList(t *testing.T) {
	// Explicit Update with empty list is valid — effectively clears the
	// node's window without fully evicting it. Lookup returns miss.
	c := NewRoutingCache()
	c.Update("node-a", []string{"rcpt-1"})
	c.Update("node-a", []string{})

	_, ok := c.Lookup("rcpt-1")
	require.False(t, ok)
}

func TestRoutingCache_Concurrent_ReadersAndWriters(t *testing.T) {
	// 20 readers + 5 writers hammering the cache. With the read/write lock,
	// no data race, no panic, every Lookup after the final Update returns the
	// expected node.
	c := NewRoutingCache()

	const (
		writers     = 5
		readers     = 20
		iterations  = 200
		perNodeSize = 50
	)

	var wg sync.WaitGroup
	wg.Add(writers)
	for w := 0; w < writers; w++ {
		go func(nodeIdx int) {
			defer wg.Done()
			node := fmt.Sprintf("node-%d", nodeIdx)
			for i := 0; i < iterations; i++ {
				// Fresh allocation per Update — matches the production
				// contract (gossip receiver deserializes a new slice per tick).
				ids := make([]string, perNodeSize)
				for j := 0; j < perNodeSize; j++ {
					ids[j] = fmt.Sprintf("rcpt-%d-%d-%d", nodeIdx, i, j)
				}
				c.Update(node, ids)
			}
		}(w)
	}

	wg.Add(readers)
	for r := 0; r < readers; r++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations*perNodeSize; i++ {
				c.Lookup(fmt.Sprintf("rcpt-some-%d", i))
			}
		}()
	}

	wg.Wait()

	// After all writers finish, the final Update from each writer left
	// perNodeSize known IDs discoverable. Verify the last writer's IDs.
	for w := 0; w < writers; w++ {
		expectedID := fmt.Sprintf("rcpt-%d-%d-0", w, iterations-1)
		node, ok := c.Lookup(expectedID)
		require.True(t, ok, "final Update should leave IDs discoverable: %s", expectedID)
		require.Equal(t, fmt.Sprintf("node-%d", w), node)
	}
}
