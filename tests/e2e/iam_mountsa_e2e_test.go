package e2e

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIAMMountSAE2E validates the MountSA admin plane (create/list/get/delete,
// policy attach/detach, cross-namespace guard) against both single-node and
// cluster fixtures.
func TestIAMMountSAE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIAMMountSACases(t, newSingleNodeIAMAdminTarget())
	})
	t.Run("Cluster4Node", func(t *testing.T) {
		runIAMMountSACases(t, newSharedClusterIAMAdminTarget(t))
	})
}

// mountSANameFor produces a short, unique MountSA name for the given target + case.
func mountSANameFor(tgtName, caseName string) string {
	return "e2e-msa-" + sanitizeForBucket(tgtName) + "-" + sanitizeForBucket(caseName)
}

// runIAMMountSACases exercises MountSA CRUD, policy attach/detach, and
// cross-namespace guard against the given target.
func runIAMMountSACases(t *testing.T, tgt iamAdminTarget) {
	t.Helper()
	ctx := context.Background()

	// CreateListGetDelete: full CRUD round-trip.
	t.Run("CreateListGetDelete", func(t *testing.T) {
		c := tgt.iamClient()
		name := mountSANameFor(tgt.name, "crud")
		t.Cleanup(func() { _ = c.MountSADelete(ctx, name) })

		created, err := c.MountSACreate(ctx, name, 1001, "e2e-test")
		require.NoError(t, err)
		assert.Equal(t, name, created.Name)
		assert.Equal(t, uint32(1001), created.UID)

		// List must contain the new entry.
		items, err := c.MountSAList(ctx)
		require.NoError(t, err)
		found := false
		for _, it := range items {
			if it.Name == name {
				found = true
				assert.Equal(t, uint32(1001), it.UID)
			}
		}
		require.True(t, found, "mount-sa %q must appear in list", name)

		// Get must return the entry.
		got, err := c.MountSAGet(ctx, name)
		require.NoError(t, err)
		assert.Equal(t, name, got.Name)
		assert.Equal(t, uint32(1001), got.UID)

		// Delete must succeed.
		require.NoError(t, c.MountSADelete(ctx, name))

		// Get on deleted entry must return 404.
		_, err = c.MountSAGet(ctx, name)
		require.Error(t, err, "Get on deleted mount-sa must error")
	})

	// PolicyAttachDetach: attach the built-in NFSMountOnly, then detach it.
	t.Run("PolicyAttachDetach", func(t *testing.T) {
		c := tgt.iamClient()
		name := mountSANameFor(tgt.name, "policy-attach")
		t.Cleanup(func() { _ = c.MountSADelete(ctx, name) })

		_, err := c.MountSACreate(ctx, name, 1002, "")
		require.NoError(t, err)

		require.NoError(t, c.MountSAPolicyAttach(ctx, name, "NFSMountOnly"))
		require.NoError(t, c.MountSAPolicyDetach(ctx, name, "NFSMountOnly"))
	})

	// CrossNamespaceGuard: attaching an S3-SA builtin to a MountSA must be
	// rejected (403 Forbidden via ValidateForMountSAAttach).
	t.Run("CrossNamespaceGuard_RejectS3Policy", func(t *testing.T) {
		c := tgt.iamClient()
		name := mountSANameFor(tgt.name, "xns-guard")
		t.Cleanup(func() { _ = c.MountSADelete(ctx, name) })

		_, err := c.MountSACreate(ctx, name, 1003, "")
		require.NoError(t, err)

		// "readonly" is an S3/Iceberg policy — must be rejected for MountSA attach.
		err = c.MountSAPolicyAttach(ctx, name, "readonly")
		require.Error(t, err, "attaching S3 policy to MountSA must be rejected")
	})

	// GetNotFound: GET on a non-existent MountSA must return a non-nil error.
	t.Run("GetNotFound", func(t *testing.T) {
		c := tgt.iamClient()
		_, err := c.MountSAGet(ctx, "does-not-exist-"+sanitizeForBucket(tgt.name))
		require.Error(t, err, "GET on missing mount-sa must error")
	})
}
