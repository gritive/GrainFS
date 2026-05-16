package server

import (
	"bytes"
	"encoding/xml"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

type deleteObjectsTestResult struct {
	Deleted []deleteObjectsTestDeleted `xml:"Deleted"`
}

type deleteObjectsTestDeleted struct {
	Key string `xml:"Key"`
}

func TestDeleteObjectsDeletesRequestedKeys(t *testing.T) {
	base := setupTestServer(t)

	req, _ := http.NewRequest(http.MethodPut, base+"/bucket", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	for _, key := range []string{"one.txt", "two.txt"} {
		req, _ = http.NewRequest(http.MethodPut, base+"/bucket/"+key, bytes.NewReader([]byte("data")))
		req.Header.Set("x-amz-acl", "public-read")
		resp, err = http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	body := []byte(`<Delete><Object><Key>one.txt</Key></Object><Object><Key>missing.txt</Key></Object></Delete>`)
	req, _ = http.NewRequest(http.MethodPost, base+"/bucket?delete", bytes.NewReader(body))
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	respBody, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, string(respBody))

	var result deleteObjectsTestResult
	require.NoError(t, xml.Unmarshal(respBody, &result))
	require.ElementsMatch(t, []deleteObjectsTestDeleted{
		{Key: "one.txt"},
		{Key: "missing.txt"},
	}, result.Deleted)

	req, _ = http.NewRequest(http.MethodHead, base+"/bucket/one.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusNotFound, resp.StatusCode)

	req, _ = http.NewRequest(http.MethodHead, base+"/bucket/two.txt", nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
}
