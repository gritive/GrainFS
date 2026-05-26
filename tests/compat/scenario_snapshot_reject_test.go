//go:build compat

// Phase A status: all standalone prev→current restart tests in this file
// are skipped. Phase A is green-field (no legacy <dataDir>/kek.key
// support); reactivation requires Phase B legacy-migration tooling.

package compat

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestHeadSnapshotInvisibleToOlderBinary(t *testing.T) {
	t.Skip("Phase A green-field cutover: legacy <dataDir>/kek.key is refused at boot; cross-version restart tests require Phase B migration support")
	prev := prevBinary(t)
	cur := getBinary()

	ports := uniqueFreePorts(6)
	prevHTTP, curHTTP, prevHTTP2 := ports[0], ports[1], ports[2]
	prevRaft, curRaft, prevRaft2 := ports[3], ports[4], ports[5]

	dataDir, err := os.MkdirTemp("/tmp", "compat-head-snap-*")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dataDir) })

	encKeyFile := makeSharedEncryptionKeyFile(t)

	cmd1 := startGrainfsNode(t, prev, dataDir, prevHTTP, prevRaft, encKeyFile)
	require.NoError(t, waitForPort(prevHTTP, 60*time.Second))
	time.Sleep(2 * time.Second)
	bootstrapCompatAdmin(t, dataDir, 30*time.Second)
	terminateProcess(cmd1)

	cmd2 := startGrainfsNode(t, cur, dataDir, curHTTP, curRaft, encKeyFile)
	require.NoError(t, waitForPort(curHTTP, 60*time.Second))
	time.Sleep(2 * time.Second)

	snapURL := fmt.Sprintf("http://127.0.0.1:%d/admin/snapshots", curHTTP)
	resp, err := httpPostJSON(snapURL, map[string]string{"reason": "head-format"})
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "create head snapshot")
	var snapOut struct {
		Seq uint64 `json:"seq"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&snapOut))
	terminateProcess(cmd2)

	cmd3 := startGrainfsNode(t, prev, dataDir, prevHTTP2, prevRaft2, encKeyFile)
	t.Cleanup(func() { terminateProcess(cmd3) })
	require.NoError(t, waitForPort(prevHTTP2, 60*time.Second))
	time.Sleep(2 * time.Second)

	status, body := restoreSnapshotStatus(t, prevHTTP2, snapOut.Seq)
	require.NotEqual(t, http.StatusOK, status, "old binary must not restore head snapshot body=%s", body)
	require.Contains(t, strings.ToLower(body), "snapshot")
}

func restoreSnapshotStatus(t *testing.T, httpPort int, seq uint64) (int, string) {
	t.Helper()

	url := fmt.Sprintf("http://127.0.0.1:%d/admin/snapshots/%d/restore", httpPort, seq)
	resp, err := httpPostJSON(url, nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}
