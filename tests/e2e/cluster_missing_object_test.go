package e2e

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// TestClusterMissingObjectE2E pins the S3 contract for anon GET/HEAD on a key
// that does not exist on the data plane, across SingleNode + Cluster3Node.
//
// Contract (single + cluster parity, per memory rule):
//   - GET    on never-existed key → 404 NoSuchKey
//   - HEAD   on never-existed key → 404 (no body)
//   - GET    after PUT+DELETE     → 404 NoSuchKey (the delete marker is a
//     not-found-for-the-latest, not a method-routing failure)
//
// Bugs this test covers:
//   - F#46           — cluster DELETE-then-GET previously returned 405
//     MethodNotAllowed instead of 404. The local-EC branch of
//     ClusterCoordinator.GetObject/HeadObject routes a versioned read at
//     the delete-marker version because the object-index entry carries the
//     marker's versionID; GetObjectVersion on a delete marker correctly
//     returns ErrMethodNotAllowed, but the unversioned caller must observe
//     ErrObjectNotFound → 404.
//   - missing-object-500 — cluster GET on a never-existed key previously
//     returned 500 "forward: no reachable peer" because no peer owned the
//     key. Absence-of-owner-for-key-that-never-existed must surface as
//     404 NoSuchKey, not as a transport "no reachable peer".
//
// Dual-target pattern per project memory:
//
//	TestXxxE2E + SingleNode + Cluster3Node + runXxxCases helper.
func TestClusterMissingObjectE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runClusterMissingObjectCases(t, newPhase0SingleNodeTarget(t))
	})
	t.Run("Cluster3Node", func(t *testing.T) {
		runClusterMissingObjectCases(t, newPhase0ClusterTarget(t))
	})
}

func runClusterMissingObjectCases(t *testing.T, tgt *phase0Target) {
	t.Run("GET_NeverExistedKey_Returns404", func(t *testing.T) {
		key := "/default/never-existed-" + uuid.NewString() + ".txt"
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+key, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		require.Equalf(t, http.StatusNotFound, resp.StatusCode,
			"anon GET on never-existed key must be 404 NoSuchKey (got %d body=%s)",
			resp.StatusCode, string(body))
		require.Containsf(t, string(body), "NoSuchKey",
			"404 body must carry NoSuchKey error code (got %s)", string(body))
	})

	t.Run("HEAD_NeverExistedKey_Returns404", func(t *testing.T) {
		key := "/default/never-existed-head-" + uuid.NewString() + ".txt"
		req, err := http.NewRequestWithContext(context.Background(),
			http.MethodHead, tgt.s3URL(0)+key, nil)
		require.NoError(t, err)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equalf(t, http.StatusNotFound, resp.StatusCode,
			"anon HEAD on never-existed key must be 404 (got %d)", resp.StatusCode)
	})

	t.Run("GET_DeletedKey_Returns404", func(t *testing.T) {
		key := "/default/deleted-" + uuid.NewString() + ".txt"

		// PUT — must succeed under Phase 0 anon contract.
		putReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodPut, tgt.s3URL(0)+key, bytes.NewReader([]byte("transient")))
		require.NoError(t, err)
		putResp, err := http.DefaultClient.Do(putReq)
		require.NoError(t, err)
		_ = putResp.Body.Close()
		require.Equalf(t, http.StatusOK, putResp.StatusCode,
			"anon PUT precondition for delete test (got %d)", putResp.StatusCode)

		// DELETE — should succeed (200 or 204).
		delReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodDelete, tgt.s3URL(0)+key, nil)
		require.NoError(t, err)
		delResp, err := http.DefaultClient.Do(delReq)
		require.NoError(t, err)
		_ = delResp.Body.Close()
		require.Containsf(t, []int{http.StatusOK, http.StatusNoContent}, delResp.StatusCode,
			"anon DELETE must succeed (got %d)", delResp.StatusCode)

		// GET — must observe the latest as "not found", not a 405.
		getReq, err := http.NewRequestWithContext(context.Background(),
			http.MethodGet, tgt.s3URL(0)+key, nil)
		require.NoError(t, err)
		getResp, err := http.DefaultClient.Do(getReq)
		require.NoError(t, err)
		defer getResp.Body.Close()
		body, _ := io.ReadAll(getResp.Body)
		require.Equalf(t, http.StatusNotFound, getResp.StatusCode,
			"anon GET on deleted key must be 404 NoSuchKey (got %d body=%s)",
			getResp.StatusCode, summarizeBody(body))
		require.Containsf(t, string(body), "NoSuchKey",
			"404 body must carry NoSuchKey error code (got %s)", summarizeBody(body))
	})
}

func summarizeBody(b []byte) string {
	s := strings.TrimSpace(string(b))
	if len(s) > 256 {
		return s[:256] + "..."
	}
	return fmt.Sprintf("%q", s)
}
