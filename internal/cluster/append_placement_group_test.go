package cluster

import (
	"context"
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

// TestLookupPlacementGroupForAppend_PrefersExistingObjectPG pins the fix for the
// false "placement group changed mid-request" rejection: a forwarded/plain-PUT
// object stores its real placement group (segment_backend writes group.ID), but
// the append used to send the routed data-group from context (or a "group-0"
// default). The FSM stale-placement check compares cmd.PlacementGroupID against
// the object's stored PlacementGroupID, so a routed-group != stored-PG mismatch
// falsely tripped ErrStalePlacement. The propose-time PG MUST be the object's
// own stored placement group.
func TestLookupPlacementGroupForAppend_PrefersExistingObjectPG(t *testing.T) {
	b := &DistributedBackend{}
	existing := &storage.Object{PlacementGroupID: "pg-stored"}
	// Context carries a DIFFERENT placement group (the routed data-group id).
	ctx := ContextWithPlacementGroup(context.Background(), "group-routed")
	require.Equal(t, "pg-stored", b.lookupPlacementGroupForAppend(ctx, existing),
		"append must use the object's own stored placement group, not the routed group")
}
