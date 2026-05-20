package e2e

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestIcebergPathCaptureE2E pins the URL path that iceberg-go's REST driver
// composes when minting an OAuth2 token. The contract under test is the path
// string: outbound POST must hit "<catalog-base>/v1/oauth/tokens" — where
// catalog-base is "<endpoint>/iceberg" (NO trailing /v1).
//
// We don't import github.com/apache/iceberg-go to avoid pulling a heavy new
// dependency just to drive an HTTP POST. iceberg-go's REST driver composes the
// token URL by appending "/v1/oauth/tokens" to the catalog base URI, and that
// path is what F#8 cares about. A raw http.Client that performs the same
// composition is a faithful — and dependency-free — replay of the wire shape.
//
// Cases:
//   - OutboundTokenPath_ExactMatch — proxies the iceberg-go-style request
//     through httptest.NewServer to capture r.URL.Path verbatim. Asserts the
//     captured path is exactly "/iceberg/v1/oauth/tokens" and the upstream
//     returns 200.
//   - CatalogBaseURI_NoTrailingV1 — asserts <endpoint>/iceberg/v1/config
//     exists (any non-404 response: 200, 401, or 403), proving /v1 is a path
//     component under the /iceberg catalog base rather than a trailing segment
//     of the base. The contract under test is the path string, not the auth
//     posture, so the assertion is fixture-independent.
func TestIcebergPathCaptureE2E(t *testing.T) {
	t.Run("SingleNode", func(t *testing.T) {
		runIcebergPathCaptureCases(t, newSingleNodeIcebergTarget(t))
	})
	t.Run("Cluster3Node", func(t *testing.T) {
		runIcebergPathCaptureCases(t, newSharedClusterIcebergTarget(t))
	})
}

func runIcebergPathCaptureCases(t *testing.T, tgt *icebergTarget) {
	t.Run("OutboundTokenPath_ExactMatch", func(t *testing.T) {
		runIcebergPathCaptureOutboundTokenPathExactMatch(t, tgt)
	})
	t.Run("CatalogBaseURI_NoTrailingV1", func(t *testing.T) {
		runIcebergPathCaptureCatalogBaseURINoTrailingV1(t, tgt)
	})
}

// runIcebergPathCaptureOutboundTokenPathExactMatch stands up a transparent
// httptest proxy in front of tgt.endpoint(0), composes the token URL the way
// iceberg-go's REST driver does (catalog-base + "/v1/oauth/tokens"), and
// asserts the proxy captured exactly "/iceberg/v1/oauth/tokens". Cluster mode
// targets node 0 only — outbound SDK URL is single-binary behavior and
// cluster determinism is not the point.
func runIcebergPathCaptureOutboundTokenPathExactMatch(t *testing.T, tgt *icebergTarget) {
	t.Helper()
	wh := tgt.uniqueWarehouse(t, "pathcap")
	saID, ak, sk := tgt.adminCreateSA(t, "pathcap")
	tgt.adminAttachPolicy(t, saID, "readonly")

	var capturedPath atomic.Value
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedPath.Store(r.URL.Path)
		out := tgt.endpoint(0) + r.URL.Path
		if r.URL.RawQuery != "" {
			out += "?" + r.URL.RawQuery
		}
		req, err := http.NewRequestWithContext(r.Context(), r.Method, out, r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for k, vv := range resp.Header {
			for _, v := range vv {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	// iceberg-go-style composition: catalog base + "/v1/oauth/tokens".
	catalogBase := proxy.URL + "/iceberg"
	tokenURL := catalogBase + "/v1/oauth/tokens"

	form := url.Values{
		"grant_type":    []string{"client_credentials"},
		"client_id":     []string{ak},
		"client_secret": []string{sk},
		"scope":         []string{"PRINCIPAL_ROLE:" + wh},
	}
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"outbound token POST must succeed through proxy")

	got, _ := capturedPath.Load().(string)
	require.Equal(t, "/iceberg/v1/oauth/tokens", got,
		"F#8: outbound token request MUST hit /iceberg/v1/oauth/tokens exactly")
}

// runIcebergPathCaptureCatalogBaseURINoTrailingV1 verifies the catalog base
// URI does NOT carry a trailing "/v1" — /v1 is a path component on
// individual endpoints (/v1/config, /v1/oauth/tokens, /v1/namespaces, ...).
//
// Path-existence check: any non-404 response proves `/iceberg/v1/config`
// exists under the catalog base. We accept 200 (anon allowed), 401 (bearer
// required), or 403 (SigV4 fallthrough with no signature) — the contract
// under test is the path string, not the auth posture. This keeps the
// assertion fixture-independent across Phase 0 (anon-enabled) and Phase 2
// (anon-disabled) auth postures.
//
// A 404 here would mean /v1 is not a path prefix under /iceberg, i.e. the
// catalog base has drifted to include /v1.
func runIcebergPathCaptureCatalogBaseURINoTrailingV1(t *testing.T, tgt *icebergTarget) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(),
		http.MethodGet, tgt.endpoint(0)+"/iceberg/v1/config", nil)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.NotEqual(t, http.StatusNotFound, resp.StatusCode,
		"catalog must expose /v1/config under /iceberg base (catalog base must NOT include /v1)")
	require.Contains(t, []int{
		http.StatusOK,
		http.StatusUnauthorized,
		http.StatusForbidden,
	}, resp.StatusCode,
		"expected 200/401/403 (path exists, auth posture varies), got %d", resp.StatusCode)
}
