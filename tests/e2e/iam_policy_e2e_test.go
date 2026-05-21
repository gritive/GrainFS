package e2e

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iamadmin"
)

var _ = ginkgo.Describe("IAM policy", func() {
	describeIAMPolicyContext("SingleNode", func(testing.TB) iamAdminTarget {
		return newSingleNodeIAMAdminTarget()
	})
	describeIAMPolicyContext("Cluster4Node", func(tb testing.TB) iamAdminTarget {
		return newSharedClusterIAMAdminTarget(tb)
	})
})

func describeIAMPolicyContext(name string, factory func(testing.TB) iamAdminTarget) {
	ginkgo.Context(name, func() {
		var (
			ctx context.Context
			tgt iamAdminTarget
		)

		ginkgo.BeforeEach(func() {
			ctx = context.Background()
			tgt = factory(ginkgo.GinkgoTB())
		})

		runIAMPolicyCases(func() context.Context { return ctx }, func() iamAdminTarget { return tgt })
	})
}

// validPolicyDoc is a minimal S3 IAM policy used across put/get/list cases.
const validPolicyDoc = `{
	"Version": "2012-10-17",
	"Statement": [{
		"Effect": "Allow",
		"Action": ["s3:GetObject"],
		"Resource": ["arn:aws:s3:::test-bucket/*"]
	}]
}`

func runIAMPolicyCases(getCtx func() context.Context, getTgt func() iamAdminTarget) {
	// --- Put / Get / Delete / List ---

	ginkgo.It("puts, gets, deletes, and rejects missing policies (PutGetDelete)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := "e2e-pgd-" + tgt.name
		ginkgo.DeferCleanup(func() { _ = c.PolicyDelete(ctx, name) })

		gomega.Expect(c.PolicyPut(ctx, name, []byte(validPolicyDoc))).To(gomega.Succeed())

		raw, err := c.PolicyGet(ctx, name)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(raw).NotTo(gomega.BeEmpty())

		// Round-tripped doc must be parseable.
		_, err = policy.Parse(raw)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		gomega.Expect(c.PolicyDelete(ctx, name)).To(gomega.Succeed())

		// After delete: Get must return 404.
		_, err = c.PolicyGet(ctx, name)
		requireAdminStatus(t, err, http.StatusNotFound)
	})

	ginkgo.It("lists custom policies (ListIncludesCustom)", func() {
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := "e2e-list-" + tgt.name
		ginkgo.DeferCleanup(func() { _ = c.PolicyDelete(ctx, name) })
		gomega.Expect(c.PolicyPut(ctx, name, []byte(validPolicyDoc))).To(gomega.Succeed())

		names, err := c.PolicyList(ctx)
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(names).To(gomega.ContainElement(name))
	})

	ginkgo.It("rejects writes to builtin policy names (PutBuiltinName_403)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		// "readonly" is a builtin — server must refuse with 403.
		err := c.PolicyPut(ctx, "readonly", []byte(validPolicyDoc))
		requireAdminStatus(t, err, http.StatusForbidden)
	})

	// PutEmptyName_400, AttachEmptyName_400, DetachEmptyName_400 are intentionally
	// absent: routes use path-param :name, so an empty segment produces a 404 from
	// the router before the handler guard (F32) runs. The guard IS exercised via
	// body-param routes (SimulateEmptySAID_400 below) and unit tests.

	ginkgo.It("returns 404 for missing policies (GetMissing_404)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		_, err := c.PolicyGet(ctx, "does-not-exist-e2e-"+tgt.name)
		requireAdminStatus(t, err, http.StatusNotFound)
	})

	ginkgo.It("rejects invalid policy documents (PutInvalidPolicy_400)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		// NotAction is explicitly banned by policy.Parse — the server must
		// reject this with 400 before the Raft round-trip.
		bad := []byte(`{"Version":"2012-10-17","Statement":[{"NotAction":"s3:GetObject","Resource":"*"}]}`)
		err := c.PolicyPut(ctx, "e2e-invalid-policy-"+tgt.name, bad)
		requireAdminStatus(t, err, http.StatusBadRequest)
	})

	ginkgo.It("rejects deleting builtin policies (DeleteBuiltinName_403)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		err := c.PolicyDelete(ctx, "readonly")
		requireAdminStatus(t, err, http.StatusForbidden)
	})

	// --- Attach / Detach ---

	ginkgo.It("attaches and detaches policies from service accounts (AttachDetach)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		policyName := "e2e-attach-" + tgt.name
		ginkgo.DeferCleanup(func() { _ = c.PolicyDelete(ctx, policyName) })
		gomega.Expect(c.PolicyPut(ctx, policyName, []byte(validPolicyDoc))).To(gomega.Succeed())

		saID, _, _ := tgt.uniqueSA(t, "attach-detach")

		gomega.Expect(c.PolicyAttachToSA(ctx, policyName, saID)).To(gomega.Succeed())
		gomega.Expect(c.PolicyDetachFromSA(ctx, policyName, saID)).To(gomega.Succeed())
	})

	// --- Simulate ---

	ginkgo.It("simulates allowed policy decisions (Simulate_Allow)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		policyName := "e2e-sim-allow-" + tgt.name
		ginkgo.DeferCleanup(func() { _ = c.PolicyDelete(ctx, policyName) })
		gomega.Expect(c.PolicyPut(ctx, policyName, []byte(validPolicyDoc))).To(gomega.Succeed())

		saID, _, _ := tgt.uniqueSA(t, "simulate-allow")

		gomega.Expect(c.PolicyAttachToSA(ctx, policyName, saID)).To(gomega.Succeed())
		ginkgo.DeferCleanup(func() { _ = c.PolicyDetachFromSA(ctx, policyName, saID) })

		resp, err := c.PolicySimulate(ctx, iamadmin.PolicySimulateRequest{
			SAID:     saID,
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::test-bucket/key",
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.Effect).To(gomega.Equal("Allow"))
	})

	ginkgo.It("simulates implicit deny policy decisions (Simulate_Deny)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		// SA with no policy attached → implicit deny.
		saID, _, _ := tgt.uniqueSA(t, "simulate-deny")

		resp, err := c.PolicySimulate(ctx, iamadmin.PolicySimulateRequest{
			SAID:     saID,
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::test-bucket/key",
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred())
		gomega.Expect(resp.Effect).To(gomega.Equal("Deny"))
	})

	ginkgo.It("rejects policy simulation with an empty service account id (SimulateEmptySAID_400)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		_, err := c.PolicySimulate(ctx, iamadmin.PolicySimulateRequest{
			SAID:     "",
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::test-bucket/key",
		})
		requireAdminStatus(t, err, http.StatusBadRequest)
	})

	// --- Local validation (no UDS dial) ---

	ginkgo.It("validates policy documents locally (Validate_Local)", func() {
		// Valid doc must parse without error.
		_, err := policy.Parse([]byte(validPolicyDoc))
		gomega.Expect(err).NotTo(gomega.HaveOccurred())

		// Invalid doc (bad Effect) must return an error.
		badDoc := `{"Statement":[{"Effect":"Maybe","Action":["s3:GetObject"],"Resource":["*"]}]}`
		_, err = policy.Parse([]byte(badDoc))
		gomega.Expect(err).To(gomega.HaveOccurred())
	})
}

// requireAdminStatus asserts that err is a *adminapi.Error with the given
// HTTP status code. Fails the test with a descriptive message if not.
func requireAdminStatus(t testing.TB, err error, wantStatus int) {
	t.Helper()
	gomega.Expect(err).To(gomega.HaveOccurred(), "expected an error with status %d", wantStatus)
	var aerr *adminapi.Error
	if !errors.As(err, &aerr) {
		ginkgo.Fail(fmt.Sprintf("expected *adminapi.Error, got %T: %v", err, err))
	}
	gomega.Expect(aerr.Status).To(gomega.Equal(wantStatus),
		"expected HTTP %d from admin, got %d (code=%q msg=%q)",
		wantStatus, aerr.Status, aerr.Code, aerr.Message)
}
