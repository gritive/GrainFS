package e2e

import "testing"

func runNBDCases(t *testing.T, tgt *nbdTarget) {
	t.Run("ReadWriteRoundTrip", func(t *testing.T) {
		device := tgt.uniqueDevice(t, "rw-roundtrip", 4*1024*1024)

		// Cluster fixture requires an explicit admin grant for the internal
		// volume bucket. Volume creation itself owns the bucket lifecycle.
		if tgt.isCluster && tgt.cluster != nil {
			tgt.cluster.GrantAdminOnBuckets("__grainfs_volumes")
		}

		client := dialE2ENBD(t, tgt.nbdAddr(0), device)
		defer client.Close()

		body := []byte("nbd-matrix-roundtrip-payload")
		client.WriteAt(t, 0, body)
		client.Flush(t)
		requireNBDReadEventually(t, client, 0, body)
	})
}

// TestNBDMatrixE2E runs the NBD matrix case set against the single-node and
// the shared 4-node cluster fixtures, mirroring the TestBucketsE2E dual-target
// convention so the same NBD client-side behaviors are exercised against both
// deployment shapes.
func TestNBDMatrixE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runNBDCases(t, newSingleNodeNBDTarget(t))
	})

	t.Run("Cluster4Node", func(t *testing.T) {
		runNBDCases(t, newSharedClusterNBDTarget(t))
	})
}
