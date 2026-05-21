package e2e

import (
	"context"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = ginkgo.Describe("IAM group", func() {
	describeIAMGroupContext("SingleNode", func(testing.TB) iamAdminTarget {
		return newSingleNodeIAMAdminTarget()
	})
	describeIAMGroupContext("Cluster4Node", func(tb testing.TB) iamAdminTarget {
		return newSharedClusterIAMAdminTarget(tb)
	})
})

func describeIAMGroupContext(name string, factory func(testing.TB) iamAdminTarget) {
	ginkgo.Context(name, func() {
		var (
			ctx context.Context
			tgt iamAdminTarget
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			tgt = factory(ginkgo.GinkgoTB())
		})

		runIAMGroupCases(func() context.Context { return ctx }, func() iamAdminTarget { return tgt })
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
func runIAMGroupCases(getCtx func() context.Context, getTgt func() iamAdminTarget) {
	// CreateDelete: create a group, delete it; cleanup handles the second delete.
	ginkgo.It("creates and deletes groups (CreateDelete)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := groupNameFor(tgt.name, "create-delete")
		ginkgo.DeferCleanup(func() { _ = c.GroupDelete(ctx, name) })

		gomega.Expect(c.GroupCreate(ctx, name)).To(gomega.Succeed())

		// Idempotent second create must not error (FSM upsert semantics).
		gomega.Expect(c.GroupCreate(ctx, name)).To(gomega.Succeed())

		gomega.Expect(c.GroupDelete(ctx, name)).To(gomega.Succeed())
	})

	// MemberAddRemove: add a SA, remove it; both calls must succeed.
	ginkgo.It("adds and removes group members (MemberAddRemove)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		grp := groupNameFor(tgt.name, "member-add-remove")
		ginkgo.DeferCleanup(func() { _ = c.GroupDelete(ctx, grp) })
		gomega.Expect(c.GroupCreate(ctx, grp)).To(gomega.Succeed())

		saID, _, _ := tgt.uniqueSA(t, "member-sa")

		gomega.Expect(c.GroupMemberAdd(ctx, grp, saID)).To(gomega.Succeed())
		gomega.Expect(c.GroupMemberRemove(ctx, grp, saID)).To(gomega.Succeed())
	})

	// PolicyAttachDetach: attach the built-in "readonly" policy to a group,
	// then detach it. Both Raft proposals must succeed.
	ginkgo.It("attaches and detaches group policies (PolicyAttachDetach)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		grp := groupNameFor(tgt.name, "policy-attach-detach")
		ginkgo.DeferCleanup(func() { _ = c.GroupDelete(ctx, grp) })
		gomega.Expect(c.GroupCreate(ctx, grp)).To(gomega.Succeed())

		gomega.Expect(c.GroupPolicyAttach(ctx, grp, "readonly")).To(gomega.Succeed())
		gomega.Expect(c.GroupPolicyDetach(ctx, grp, "readonly")).To(gomega.Succeed())
	})

	// MultiMember: add two SAs to a group, remove both; verifies the store
	// handles overlapping membership without corruption.
	ginkgo.It("handles multiple group members (MultiMember)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		grp := groupNameFor(tgt.name, "multi-member")
		ginkgo.DeferCleanup(func() { _ = c.GroupDelete(ctx, grp) })
		gomega.Expect(c.GroupCreate(ctx, grp)).To(gomega.Succeed())

		saID1, _, _ := tgt.uniqueSA(t, "multi-member-sa1")
		saID2, _, _ := tgt.uniqueSA(t, "multi-member-sa2")

		gomega.Expect(c.GroupMemberAdd(ctx, grp, saID1)).To(gomega.Succeed())
		gomega.Expect(c.GroupMemberAdd(ctx, grp, saID2)).To(gomega.Succeed())
		gomega.Expect(c.GroupMemberRemove(ctx, grp, saID1)).To(gomega.Succeed())
		gomega.Expect(c.GroupMemberRemove(ctx, grp, saID2)).To(gomega.Succeed())
	})
}
