package server

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPatchConfig_InvalidJSON(t *testing.T) {
	base := setupTestServer(t)

	resp, err := http.DefaultClient.Do(newPatch(t, base+"/api/admin/config", []byte(`not-json`)))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPatchConfig_InvalidDuration(t *testing.T) {
	base := setupTestServer(t)

	body, _ := json.Marshal(map[string]string{"scrub_interval": "notaduration"})
	resp, err := http.DefaultClient.Do(newPatch(t, base+"/api/admin/config", body))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestPatchConfig_ValidScrubInterval(t *testing.T) {
	base := setupTestServer(t)

	body, _ := json.Marshal(map[string]string{"scrub_interval": "2m"})
	resp, err := http.DefaultClient.Do(newPatch(t, base+"/api/admin/config", body))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var result configPatchResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&result))
	assert.True(t, result.Applied)
	assert.Equal(t, "2m0s", result.ScrubInterval)
}

func newPatch(t *testing.T, url string, body []byte) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodPatch, url, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	return req
}
