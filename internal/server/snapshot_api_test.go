package server

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/encrypt"
	"github.com/gritive/GrainFS/internal/snapshot"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/klauspost/compress/zstd"
)

// testSnapshotKEK returns a test KEKStore and cluster ID for server snapshot tests.
func testSnapshotKEK(t *testing.T) (snapshot.KEKSource, [16]byte) {
	t.Helper()
	store := encrypt.NewKEKStore()
	require.NoError(t, store.Add(1, make([]byte, encrypt.KEKSize)))
	var cid [16]byte
	cid[0] = 0x5A
	return store, cid
}

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
	kek, cid := testSnapshotKEK(t)
	srv := New(addr, backend, WithDataDir(dataDir), WithSnapshotKEK(kek, cid))
	go srv.Run() //nolint:errcheck
	t.Cleanup(func() {
		shutdownTestServer(t, srv)
	})
	waitForTCP(t, addr)

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
	kek, cid := testSnapshotKEK(t)
	srv := New(addr, backend, WithDataDir(dataDir), WithSnapshotKEK(kek, cid))
	go srv.Run() //nolint:errcheck
	t.Cleanup(func() {
		shutdownTestServer(t, srv)
	})
	waitForTCP(t, addr)

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

	// Write a legacy plaintext GFSNAP01 file with a future minReader version
	// (bypasses envelope — exercises the legacy read-compat shim).
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
