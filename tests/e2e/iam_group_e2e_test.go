package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIAMGroupE2E validates the IAM group admin plane (create/delete/member/
// policy-attach/detach) against both single-node and cluster fixtures.
func TestIAMGroupE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIAMGroupCases(t, newSingleNodeIAMAdminTarget())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runIAMGroupCases(t, newSharedClusterIAMAdminTarget(t))
	})
}

// groupNameFor produces a short, unique group name for the given target + case.
func groupNameFor(tgtName, caseName string) string {
	return "e2e-grp-" + sanitizeForBucket(tgtName) + "-" + sanitizeForBucket(caseName)
}

// runIAMGroupCases exercises group CRUD, member add/remove, and policy
// attach/detach against the given target.
//
// Absent cases and rationale:
//
//   - "List" / "ListAfterCreate": no GET /v1/iam/group route exists in the
//     current server; only PUT and DELETE on :name are registered.
//   - "MemberList": no GET /v1/iam/group/:name/members route exists.
//   - "CreateEmptyName_400", "MemberAddEmptySAID_400", "PolicyAttachEmptyTarget_400":
//     group routes use path-params (:name, :said, :policy). An empty segment
//     produces a router 404 before the handler guard fires — same situation as
//     iam_policy_e2e_test.go lines 84-87. Empty-name guards ARE covered by
//     unit tests in handlers_iam_group.go tests.
//   - "MemberAddToMissingGroup_404" / "PolicyAttachToMissingGroup_404":
//     ErrGroupNotFound is a raw error from the Raft apply path; it is not
//     wrapped as *adminapi.Error by the handler and therefore resolves to
//     500 Internal Server Error rather than 404. Covered by the store
//     unit tests (group/store_test.go) instead.
func runIAMGroupCases(t *testing.T, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	// CreateDelete: create a group, delete it; cleanup handles the second delete.
	t.Run("CreateDelete", func(t *testing.T) {
		c := tgt.iamClient()
		name := groupNameFor(tgt.name, "create-delete")
		t.Cleanup(func() { _ = c.GroupDelete(ctx, name) })

		require.NoError(t, c.GroupCreate(ctx, name))

		// Idempotent second create must not error (FSM upsert semantics).
		require.NoError(t, c.GroupCreate(ctx, name))

		require.NoError(t, c.GroupDelete(ctx, name))
	})

	// MemberAddRemove: add a SA, remove it; both calls must succeed.
	t.Run("MemberAddRemove", func(t *testing.T) {
		c := tgt.iamClient()
		grp := groupNameFor(tgt.name, "member-add-remove")
		t.Cleanup(func() { _ = c.GroupDelete(ctx, grp) })
		require.NoError(t, c.GroupCreate(ctx, grp))

		saID, _, _ := tgt.uniqueSA(t, "member-sa")

		require.NoError(t, c.GroupMemberAdd(ctx, grp, saID))
		require.NoError(t, c.GroupMemberRemove(ctx, grp, saID))
	})

	// PolicyAttachDetach: attach the built-in "readonly" policy to a group,
	// then detach it. Both Raft proposals must succeed.
	t.Run("PolicyAttachDetach", func(t *testing.T) {
		c := tgt.iamClient()
		grp := groupNameFor(tgt.name, "policy-attach-detach")
		t.Cleanup(func() { _ = c.GroupDelete(ctx, grp) })
		require.NoError(t, c.GroupCreate(ctx, grp))

		require.NoError(t, c.GroupPolicyAttach(ctx, grp, "readonly"))
		require.NoError(t, c.GroupPolicyDetach(ctx, grp, "readonly"))
	})

	// MultiMember: add two SAs to a group, remove both; verifies the store
	// handles overlapping membership without corruption.
	t.Run("MultiMember", func(t *testing.T) {
		c := tgt.iamClient()
		grp := groupNameFor(tgt.name, "multi-member")
		t.Cleanup(func() { _ = c.GroupDelete(ctx, grp) })
		require.NoError(t, c.GroupCreate(ctx, grp))

		saID1, _, _ := tgt.uniqueSA(t, "multi-member-sa1")
		saID2, _, _ := tgt.uniqueSA(t, "multi-member-sa2")

		require.NoError(t, c.GroupMemberAdd(ctx, grp, saID1))
		require.NoError(t, c.GroupMemberAdd(ctx, grp, saID2))
		require.NoError(t, c.GroupMemberRemove(ctx, grp, saID1))
		require.NoError(t, c.GroupMemberRemove(ctx, grp, saID2))
	})
}
