package cluster

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAtomicShardFileWrite_DirectIOSpikeFallbackWritesExactPayload(t *testing.T) {
	t.Setenv(shardDirectIOEnv, "1")
	origOpenDirect := openDirectShardFile
	t.Cleanup(func() { openDirectShardFile = origOpenDirect })

	attemptedDirect := false
	openDirectShardFile = func(string, int, os.FileMode) (*os.File, error) {
		attemptedDirect = true
		return nil, errors.New("direct io unavailable")
	}

	bk := newECBenchmarkBackend(t)
	l := bk.shardSvc.local
	dir, err := l.getShardDir("b", "direct-fallback", 0)
	require.NoError(t, err)
	require.NoError(t, l.ensureShardDir(dir))
	path := filepath.Join(dir, "shard_0")
	payload := []byte("direct-io-spike-fallback-payload")

	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
	require.NoError(t, err)
	require.True(t, attemptedDirect, "opt-in direct I/O spike must attempt direct open")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAtomicShardFileWrite_DirectIOSpikeDirectPathTruncatesAlignmentPadding(t *testing.T) {
	t.Setenv(shardDirectIOEnv, "1")
	origOpenDirect := openDirectShardFile
	t.Cleanup(func() { openDirectShardFile = origOpenDirect })

	attemptedDirect := false
	openDirectShardFile = func(path string, flag int, mode os.FileMode) (*os.File, error) {
		attemptedDirect = true
		return os.OpenFile(path, flag, mode)
	}

	bk := newECBenchmarkBackend(t)
	l := bk.shardSvc.local
	dir, err := l.getShardDir("b", "direct-unaligned", 0)
	require.NoError(t, err)
	require.NoError(t, l.ensureShardDir(dir))
	path := filepath.Join(dir, "shard_0")
	payload := bytes.Repeat([]byte{0x5a}, 4097)

	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
	require.NoError(t, err)
	require.True(t, attemptedDirect, "opt-in direct I/O spike must use direct open when available")

	st, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), st.Size(), "direct write padding must be truncated")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
