package cluster

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedundancyUpgradeAdapter_CaughtUpOwnerRequiresSingletonOwner(t *testing.T) {
	owner := &DistributedBackend{}
	owner.SetGCFreshnessGate(func(context.Context) bool { return true })

	b := &DistributedBackend{}
	b.SetOwningGroupBackendSource(func(string) *DistributedBackend { return owner })
	b.SetGCSingletonOwnerChecker(func(bucket string) bool { return bucket == "owned" })

	adapter := redundancyUpgradeAdapter{b: b, ctx: context.Background()}
	require.True(t, adapter.caughtUpOwner("owned"))
	require.False(t, adapter.caughtUpOwner("not-owned"))
}
