//go:build !race

package s3auth

import (
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// AllocsPerRun budgets run outside -race because race instrumentation changes
// heap escapes and allocation counts.
func TestCachingVerifier_HotPathAllocBudget(t *testing.T) {
	inner := newTestVerifier(t)
	cv := NewCachingVerifier(inner, 16, 5*time.Minute)
	req := signedRequest(t, "testkey", "testsecret")
	require.NoError(t, warmVerifierCache(cv, req))

	allocs := testing.AllocsPerRun(100, func() {
		_, err := cv.Verify(req)
		require.NoError(t, err)
	})

	require.LessOrEqual(t, allocs, 34.0, "cached SigV4 verify should stay allocation-light")
}

func warmVerifierCache(cv *CachingVerifier, req *http.Request) error {
	_, err := cv.Verify(req)
	return err
}
