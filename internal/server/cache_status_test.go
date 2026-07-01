package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cache/shardcache"
	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/server/servertest"
)

// setupCacheTestServer mirrors setupTestServer but wires the shard cache
// option so the /api/cache/status response carries real Stats. Returns the
// base URL plus the cache instance so the test can record traffic and verify
// the JSON reflects it.
func setupCacheTestServer(t *testing.T, shardCap int64) (string, *shardcache.Cache) {
	t.Helper()
	backend := cluster.NewSingletonBackendForTest(t)

	sc := shardcache.New(shardCap)

	port := servertest.FreePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithShardCache(sc))
	go srv.Run() //nolint:errcheck
	for i := 0; i < 50; i++ {
		conn, err := net.Dial("tcp", addr)
		if err == nil {
			conn.Close()
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "http://" + addr, sc
}

func getCacheStatus(t *testing.T, base string) map[string]any {
	t.Helper()
	resp, err := http.Get(base + "/api/cache/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return out
}

func TestCacheStatus_ShardCacheDisabled(t *testing.T) {
	base, _ := setupCacheTestServer(t, 0)

	status := getCacheStatus(t, base)
	sc, ok := status["shard_cache"].(map[string]any)
	require.True(t, ok, "shard_cache section missing — UI dashboard depends on it")
	assert.Equal(t, false, sc["enabled"], "disabled shard cache must report enabled:false")
}

func TestCacheStatus_ShardCacheEnabled(t *testing.T) {
	base, sc := setupCacheTestServer(t, 1024*1024)

	sc.Put("s1", []byte("shard-bytes"))
	_, _ = sc.Get("s1") // hit
	_, _ = sc.Get("s9") // miss

	status := getCacheStatus(t, base)

	scStat := status["shard_cache"].(map[string]any)
	assert.Equal(t, true, scStat["enabled"])
	assert.EqualValues(t, 1, scStat["hits"])
	assert.EqualValues(t, 1, scStat["misses"])
	assert.EqualValues(t, 50.0, scStat["hit_rate_pct"])
	assert.EqualValues(t, 1024*1024, scStat["capacity_bytes"])
}

func TestCacheStatus_AuthBypass(t *testing.T) {
	// /api/cache/status must be reachable without SigV4 — the dashboard
	// fetches without credentials and the auth-bypass list backs that.
	// Even when the server has auth wired, this endpoint stays open like
	// /metrics.
	base, _ := setupCacheTestServer(t, 1024)

	resp, err := http.Get(base + "/api/cache/status")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "cache status must not require auth")
	require.Equal(t, "application/json", resp.Header.Get("Content-Type"))
}
