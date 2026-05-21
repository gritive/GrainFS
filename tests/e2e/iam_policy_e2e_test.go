package e2e

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/onsi/ginkgo/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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

		require.NoError(t, c.PolicyPut(ctx, name, []byte(validPolicyDoc)))

		raw, err := c.PolicyGet(ctx, name)
		require.NoError(t, err)
		require.NotEmpty(t, raw)

		// Round-tripped doc must be parseable.
		_, err = policy.Parse(raw)
		require.NoError(t, err)

		require.NoError(t, c.PolicyDelete(ctx, name))

		// After delete: Get must return 404.
		_, err = c.PolicyGet(ctx, name)
		requireAdminStatus(t, err, http.StatusNotFound)
	})

	ginkgo.It("lists custom policies (ListIncludesCustom)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		name := "e2e-list-" + tgt.name
		ginkgo.DeferCleanup(func() { _ = c.PolicyDelete(ctx, name) })
		require.NoError(t, c.PolicyPut(ctx, name, []byte(validPolicyDoc)))

		names, err := c.PolicyList(ctx)
		require.NoError(t, err)
		require.Contains(t, names, name)
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
		require.NoError(t, c.PolicyPut(ctx, policyName, []byte(validPolicyDoc)))

		saID, _, _ := tgt.uniqueSA(t, "attach-detach")

		require.NoError(t, c.PolicyAttachToSA(ctx, policyName, saID))
		require.NoError(t, c.PolicyDetachFromSA(ctx, policyName, saID))
	})

	// --- Simulate ---

	ginkgo.It("simulates allowed policy decisions (Simulate_Allow)", func() {
		t := ginkgo.GinkgoTB()
		ctx := getCtx()
		tgt := getTgt()
		c := tgt.iamClient()
		policyName := "e2e-sim-allow-" + tgt.name
		ginkgo.DeferCleanup(func() { _ = c.PolicyDelete(ctx, policyName) })
		require.NoError(t, c.PolicyPut(ctx, policyName, []byte(validPolicyDoc)))

		saID, _, _ := tgt.uniqueSA(t, "simulate-allow")

		require.NoError(t, c.PolicyAttachToSA(ctx, policyName, saID))
		ginkgo.DeferCleanup(func() { _ = c.PolicyDetachFromSA(ctx, policyName, saID) })

		resp, err := c.PolicySimulate(ctx, iamadmin.PolicySimulateRequest{
			SAID:     saID,
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::test-bucket/key",
		})
		require.NoError(t, err)
		assert.Equal(t, "Allow", resp.Effect)
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
		require.NoError(t, err)
		assert.Equal(t, "Deny", resp.Effect)
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
		t := ginkgo.GinkgoTB()
		// Valid doc must parse without error.
		_, err := policy.Parse([]byte(validPolicyDoc))
		require.NoError(t, err)

		// Invalid doc (bad Effect) must return an error.
		badDoc := `{"Statement":[{"Effect":"Maybe","Action":["s3:GetObject"],"Resource":["*"]}]}`
		_, err = policy.Parse([]byte(badDoc))
		require.Error(t, err)
	})
}

// requireAdminStatus asserts that err is a *adminapi.Error with the given
// HTTP status code. Fails the test with a descriptive message if not.
func requireAdminStatus(t testing.TB, err error, wantStatus int) {
	t.Helper()
	require.Error(t, err, "expected an error with status %d", wantStatus)
	var aerr *adminapi.Error
	if !errors.As(err, &aerr) {
		t.Fatalf("expected *adminapi.Error, got %T: %v", err, err)
	}
	require.Equalf(t, wantStatus, aerr.Status,
		"expected HTTP %d from admin, got %d (code=%q msg=%q)",
		wantStatus, aerr.Status, aerr.Code, aerr.Message)
}
