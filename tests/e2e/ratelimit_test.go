package e2e

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestE2E_RateLimit_NotTriggeredUnderNormalLoad(t *testing.T) {
	// Normal requests should not be rate limited
	for i := 0; i < 10; i++ {
		req, _ := http.NewRequest(http.MethodGet, testServerURL+"/", nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		assert.Equal(t, http.StatusOK, resp.StatusCode, "request %d should not be rate limited", i)
	}
}
