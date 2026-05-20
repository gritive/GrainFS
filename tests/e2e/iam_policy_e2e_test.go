package e2e

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iamadmin"
)

// TestIAMPolicyE2E validates the IAM policy admin plane (PUT/GET/DELETE/LIST/
// ATTACH/DETACH/SIMULATE) against both single-node and cluster fixtures.
func TestIAMPolicyE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIAMPolicyCases(t, newSingleNodeIAMAdminTarget())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runIAMPolicyCases(t, newSharedClusterIAMAdminTarget(t))
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

func runIAMPolicyCases(t *testing.T, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	// --- Put / Get / Delete / List ---

	t.Run("PutGetDelete", func(t *testing.T) {
		c := tgt.iamClient()
		name := "e2e-pgd-" + tgt.name
		t.Cleanup(func() { _ = c.PolicyDelete(ctx, name) })

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

	t.Run("ListIncludesCustom", func(t *testing.T) {
		c := tgt.iamClient()
		name := "e2e-list-" + tgt.name
		t.Cleanup(func() { _ = c.PolicyDelete(ctx, name) })
		require.NoError(t, c.PolicyPut(ctx, name, []byte(validPolicyDoc)))

		names, err := c.PolicyList(ctx)
		require.NoError(t, err)
		require.Contains(t, names, name)
	})

	t.Run("PutBuiltinName_403", func(t *testing.T) {
		c := tgt.iamClient()
		// "readonly" is a builtin — server must refuse with 403.
		err := c.PolicyPut(ctx, "readonly", []byte(validPolicyDoc))
		requireAdminStatus(t, err, http.StatusForbidden)
	})

	// PutEmptyName_400, AttachEmptyName_400, DetachEmptyName_400 are intentionally
	// absent: routes use path-param :name, so an empty segment produces a 404 from
	// the router before the handler guard (F32) runs. The guard IS exercised via
	// body-param routes (SimulateEmptySAID_400 below) and unit tests.

	t.Run("GetMissing_404", func(t *testing.T) {
		c := tgt.iamClient()
		_, err := c.PolicyGet(ctx, "does-not-exist-e2e-"+tgt.name)
		requireAdminStatus(t, err, http.StatusNotFound)
	})

	t.Run("PutInvalidPolicy_400", func(t *testing.T) {
		c := tgt.iamClient()
		// NotAction is explicitly banned by policy.Parse — the server must
		// reject this with 400 before the Raft round-trip.
		bad := []byte(`{"Version":"2012-10-17","Statement":[{"NotAction":"s3:GetObject","Resource":"*"}]}`)
		err := c.PolicyPut(ctx, "e2e-invalid-policy-"+tgt.name, bad)
		requireAdminStatus(t, err, http.StatusBadRequest)
	})

	t.Run("DeleteBuiltinName_403", func(t *testing.T) {
		c := tgt.iamClient()
		err := c.PolicyDelete(ctx, "readonly")
		requireAdminStatus(t, err, http.StatusForbidden)
	})

	// --- Attach / Detach ---

	t.Run("AttachDetach", func(t *testing.T) {
		c := tgt.iamClient()
		policyName := "e2e-attach-" + tgt.name
		t.Cleanup(func() { _ = c.PolicyDelete(ctx, policyName) })
		require.NoError(t, c.PolicyPut(ctx, policyName, []byte(validPolicyDoc)))

		saID, _, _ := tgt.uniqueSA(t, "attach-detach")

		require.NoError(t, c.PolicyAttachToSA(ctx, policyName, saID))
		require.NoError(t, c.PolicyDetachFromSA(ctx, policyName, saID))
	})

	// --- Simulate ---

	t.Run("Simulate_Allow", func(t *testing.T) {
		c := tgt.iamClient()
		policyName := "e2e-sim-allow-" + tgt.name
		t.Cleanup(func() { _ = c.PolicyDelete(ctx, policyName) })
		require.NoError(t, c.PolicyPut(ctx, policyName, []byte(validPolicyDoc)))

		saID, _, _ := tgt.uniqueSA(t, "simulate-allow")

		require.NoError(t, c.PolicyAttachToSA(ctx, policyName, saID))
		t.Cleanup(func() { _ = c.PolicyDetachFromSA(ctx, policyName, saID) })

		resp, err := c.PolicySimulate(ctx, iamadmin.PolicySimulateRequest{
			SAID:     saID,
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::test-bucket/key",
		})
		require.NoError(t, err)
		assert.Equal(t, "Allow", resp.Effect)
	})

	t.Run("Simulate_Deny", func(t *testing.T) {
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

	t.Run("SimulateEmptySAID_400", func(t *testing.T) {
		c := tgt.iamClient()
		_, err := c.PolicySimulate(ctx, iamadmin.PolicySimulateRequest{
			SAID:     "",
			Action:   "s3:GetObject",
			Resource: "arn:aws:s3:::test-bucket/key",
		})
		requireAdminStatus(t, err, http.StatusBadRequest)
	})

	// --- Local validation (no UDS dial) ---

	t.Run("Validate_Local", func(t *testing.T) {
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
func requireAdminStatus(t *testing.T, err error, wantStatus int) {
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
