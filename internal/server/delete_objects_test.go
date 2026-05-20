package server

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

type deleteObjectsTestResult struct {
	Deleted []deleteObjectsTestDeleted `xml:"Deleted"`
}

type deleteObjectsTestDeleted struct {
	Key string `xml:"Key"`
}

func TestDeleteObjectsDeletesRequestedKeys(t *testing.T) {
	base, backend := setupTestServerWithBackend(t)
	mustCreateBucket(t, backend, "bucket")

	for _, key := range []string{"one.txt", "two.txt"} {
		req, _ := http.NewRequest(http.MethodPut, base+"/bucket/"+key, bytes.NewReader([]byte("data")))
		req.Header.Set("x-amz-acl", "public-read")
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	body := []byte(`<Delete><Object><Key>one.txt</Key></Object><Object><Key>missing.txt</Key></Object></Delete>`)
	req, _ := http.NewRequest(http.MethodPost, base+"/bucket?delete", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
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

func TestDeleteObjectsDeletesBatchConcurrently(t *testing.T) {
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { local.Close() })

	backend := &slowDeleteBackend{
		LocalBackend: local,
		delay:        50 * time.Millisecond,
	}
	addr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	startTestServer(t, addr, backend)
	base := "http://" + addr

	mustCreateBucket(t, local, "bucket")
	for _, key := range []string{"a.txt", "b.txt", "c.txt", "d.txt"} {
		_, err := local.PutObject(t.Context(), "bucket", key, strings.NewReader("data"), "text/plain")
		require.NoError(t, err)
	}

	body := []byte(`<Delete><Object><Key>a.txt</Key></Object><Object><Key>b.txt</Key></Object><Object><Key>c.txt</Key></Object><Object><Key>d.txt</Key></Object></Delete>`)
	req, _ := http.NewRequest(http.MethodPost, base+"/bucket?delete", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	require.GreaterOrEqual(t, backend.maxInFlight.Load(), int32(2), "multi-object delete should not process the batch serially")
}

type slowDeleteBackend struct {
	*storage.LocalBackend
	delay       time.Duration
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
}

func (b *slowDeleteBackend) DeleteObject(ctx context.Context, bucket, key string) error {
	current := b.inFlight.Add(1)
	for {
		max := b.maxInFlight.Load()
		if current <= max || b.maxInFlight.CompareAndSwap(max, current) {
			break
		}
	}
	time.Sleep(b.delay)
	defer b.inFlight.Add(-1)
	return b.LocalBackend.DeleteObject(ctx, bucket, key)
}
