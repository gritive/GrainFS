package main

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/volume"
)

type flakyVolumeMetaBackend struct {
	storage.Backend
	failPuts atomic.Int32
}

func (b *flakyVolumeMetaBackend) PutObject(ctx context.Context, bucket, key string, r io.Reader, contentType string) (*storage.Object, error) {
	if bucket == "__grainfs_volumes" && key == "__vol/default/meta" && b.failPuts.Add(-1) >= 0 {
		return nil, storage.ErrNoSuchBucket
	}
	return b.Backend.PutObject(ctx, bucket, key, r, contentType)
}

func TestEnsureDefaultNBDVolumeRetriesTransientCreateFailure(t *testing.T) {
	dir := t.TempDir()
	local, err := storage.NewLocalBackend(dir)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, local.Close()) })

	backend := &flakyVolumeMetaBackend{Backend: local}
	backend.failPuts.Store(1)
	mgr := volume.NewManager(backend)

	err = ensureDefaultNBDVolume(context.Background(), mgr, "default", 4096, 2*time.Second)
	require.NoError(t, err)

	vol, err := mgr.Get("default")
	require.NoError(t, err)
	require.EqualValues(t, 4096, vol.Size)
}
