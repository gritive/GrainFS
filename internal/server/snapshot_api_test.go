package server

import (
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/klauspost/compress/zstd"
)

func TestRestoreSnapshotUnsupportedFormatReturnsConflict(t *testing.T) {
	dataDir := t.TempDir()
	snapshotDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0o755))
	writeFutureSnapshotAPIFile(t, filepath.Join(snapshotDir, "snapshot-1.json.zst"))

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithDataDir(dataDir))
	go srv.Run() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	waitForTestPort(t, addr)

	resp, err := http.Post("http://"+addr+"/admin/snapshots/1/restore", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)
	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "unsupported snapshot format", body["error"])
	require.Contains(t, body["hint"], "min reader format")
}

func TestRestoreLegacyGzipSnapshotReturnsConflict(t *testing.T) {
	dataDir := t.TempDir()
	snapshotDir := filepath.Join(dataDir, "snapshots")
	require.NoError(t, os.MkdirAll(snapshotDir, 0o755))
	writeLegacySnapshotAPIFile(t, filepath.Join(snapshotDir, "snapshot-1.json.gz"))

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	port := freePort(t)
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	srv := New(addr, backend, WithDataDir(dataDir))
	go srv.Run() //nolint:errcheck
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	})
	waitForTestPort(t, addr)

	resp, err := http.Post("http://"+addr+"/admin/snapshots/1/restore", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusConflict, resp.StatusCode)
	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	require.Equal(t, "unsupported snapshot format", body["error"])
}

func writeFutureSnapshotAPIFile(t *testing.T, path string) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte("GFSNAP01"))
	require.NoError(t, err)
	require.NoError(t, binary.Write(f, binary.BigEndian, uint32(2)))
	require.NoError(t, binary.Write(f, binary.BigEndian, uint32(2)))
	require.NoError(t, binary.Write(f, binary.BigEndian, time.Now().UnixNano()))

	zw, err := zstd.NewWriter(f, zstd.WithEncoderLevel(zstd.SpeedDefault))
	require.NoError(t, err)
	require.NoError(t, json.NewEncoder(zw).Encode(&snapshot.Snapshot{Seq: 1}))
	require.NoError(t, zw.Close())
	require.NoError(t, f.Close())
}

func writeLegacySnapshotAPIFile(t *testing.T, path string) {
	t.Helper()

	f, err := os.Create(path)
	require.NoError(t, err)
	gz := gzip.NewWriter(f)
	require.NoError(t, json.NewEncoder(gz).Encode(&snapshot.Snapshot{Seq: 1}))
	require.NoError(t, gz.Close())
	require.NoError(t, f.Close())
}

func waitForTestPort(t *testing.T, addr string) {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		if !strings.Contains(err.Error(), "connection refused") {
			t.Logf("waiting for %s: %v", addr, err)
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("server %s did not start", addr)
}
