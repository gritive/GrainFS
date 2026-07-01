package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

type recordingDirectShardFile struct {
	writes      [][]byte
	truncates   []int64
	writeErr    error
	truncateErr error
}

func (f *recordingDirectShardFile) Write(p []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	cp := append([]byte(nil), p...)
	f.writes = append(f.writes, cp)
	return len(p), nil
}

func (f *recordingDirectShardFile) Truncate(size int64) error {
	if f.truncateErr != nil {
		return f.truncateErr
	}
	f.truncates = append(f.truncates, size)
	return nil
}

func TestDirectShardStreamWriter_WritesFullBlocksBeforeClose(t *testing.T) {
	rec := &recordingDirectShardFile{}
	w := newDirectShardStreamWriter(rec, 4096)
	payload := append(bytes.Repeat([]byte{0xa5}, 4096), bytes.Repeat([]byte{0x5a}, 17)...)

	n, err := w.Write(payload)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Len(t, rec.writes, 1, "full aligned block must stream before Close")
	require.Len(t, rec.writes[0], 4096)
	require.Equal(t, payload[:4096], rec.writes[0])

	require.NoError(t, w.Close())
	require.Equal(t, int64(len(payload)), w.BytesWritten())
	require.Len(t, rec.writes, 2)
	require.Len(t, rec.writes[1], 4096)
	require.Equal(t, payload[4096:], rec.writes[1][:17])
	require.Equal(t, bytes.Repeat([]byte{0}, 4096-17), rec.writes[1][17:])
	require.Equal(t, []int64{int64(len(payload))}, rec.truncates)
}

func TestAtomicShardFileWrite_DirectIOEnabledByDefault(t *testing.T) {
	t.Setenv(shardDirectIOEnv, "")
	origOpenDirect := openDirectShardFile
	t.Cleanup(func() { openDirectShardFile = origOpenDirect })

	attemptedDirect := false
	openDirectShardFile = func(path string, flag int, mode os.FileMode) (*os.File, error) {
		attemptedDirect = true
		return os.OpenFile(path, flag, mode)
	}

	bk := newECBenchmarkBackend(t)
	l := bk.shardSvc.local
	dir, err := l.getShardDir("b", "direct-default", 0)
	require.NoError(t, err)
	require.NoError(t, l.ensureShardDir(dir))
	path := filepath.Join(dir, "shard_0")
	payload := bytes.Repeat([]byte{0x42}, 4096)

	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, -1, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
	require.NoError(t, err)
	require.True(t, attemptedDirect, "direct I/O should be the default shard write path")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAtomicShardFileWrite_DirectIOCanBeDisabled(t *testing.T) {
	t.Setenv(shardDirectIOEnv, "0")
	origOpenDirect := openDirectShardFile
	t.Cleanup(func() { openDirectShardFile = origOpenDirect })

	attemptedDirect := false
	openDirectShardFile = func(string, int, os.FileMode) (*os.File, error) {
		attemptedDirect = true
		return nil, errors.New("direct io should be disabled")
	}

	bk := newECBenchmarkBackend(t)
	l := bk.shardSvc.local
	dir, err := l.getShardDir("b", "direct-disabled", 0)
	require.NoError(t, err)
	require.NoError(t, l.ensureShardDir(dir))
	path := filepath.Join(dir, "shard_0")
	payload := []byte("direct-io-disabled-payload")

	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, -1, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
	require.NoError(t, err)
	require.False(t, attemptedDirect, "explicit falsey env must disable direct I/O")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAtomicShardFileWrite_DirectIOStreamsDuringWriteBody(t *testing.T) {
	t.Setenv(shardDirectIOEnv, "1")
	origOpenDirect := openDirectShardFile
	t.Cleanup(func() { openDirectShardFile = origOpenDirect })

	openDirectShardFile = func(path string, flag int, mode os.FileMode) (*os.File, error) {
		return os.OpenFile(path, flag, mode)
	}

	bk := newECBenchmarkBackend(t)
	l := bk.shardSvc.local
	dir, err := l.getShardDir("b", "direct-streaming", 0)
	require.NoError(t, err)
	require.NoError(t, l.ensureShardDir(dir))
	path := filepath.Join(dir, "shard_0")
	payload := bytes.Repeat([]byte{0x7c}, 4096)

	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, -1, func(w io.Writer) error {
		_, err := w.Write(payload)
		if err != nil {
			return err
		}
		matches, err := filepath.Glob(path + ".*.tmp")
		if err != nil {
			return err
		}
		if len(matches) != 1 {
			return fmt.Errorf("tmp files during writeBody = %d, want 1", len(matches))
		}
		st, err := os.Stat(matches[0])
		if err != nil {
			return err
		}
		if st.Size() == 0 {
			return errors.New("direct tmp file was not written during writeBody")
		}
		return nil
	})
	require.NoError(t, err)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAtomicShardFileWrite_DirectIOFallbackWritesExactPayload(t *testing.T) {
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

	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, -1, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
	require.NoError(t, err)
	require.True(t, attemptedDirect, "direct I/O must attempt direct open when enabled")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}

func TestAtomicShardFileWrite_DirectIODirectPathTruncatesAlignmentPadding(t *testing.T) {
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

	err = l.atomicShardFileWrite(context.Background(), dir, path, 0, -1, func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
	require.NoError(t, err)
	require.True(t, attemptedDirect, "direct I/O must use direct open when available")

	st, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(len(payload)), st.Size(), "direct write padding must be truncated")

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, payload, got)
}
