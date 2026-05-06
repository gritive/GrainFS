package serveruntime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/storage"
)

type readinessSnapshotBackend struct {
	failures int
	calls    int
}

func (b *readinessSnapshotBackend) ListAllObjects() ([]storage.SnapshotObject, error) {
	b.calls++
	if b.calls <= b.failures {
		return nil, errors.New("snapshot route not ready")
	}
	return nil, nil
}

func (b *readinessSnapshotBackend) RestoreObjects([]storage.SnapshotObject) (int, []storage.StaleBlob, error) {
	return 0, nil, nil
}

func TestWaitForSnapshotBackendReady_RetriesUntilRouteReady(t *testing.T) {
	backend := &readinessSnapshotBackend{failures: 2}

	err := waitForSnapshotBackendReady(context.Background(), backend, time.Second)
	require.NoError(t, err)
	require.GreaterOrEqual(t, backend.calls, 3)
}

func TestWaitForSnapshotBackendReady_TimesOut(t *testing.T) {
	backend := &readinessSnapshotBackend{failures: 1000}

	err := waitForSnapshotBackendReady(context.Background(), backend, 10*time.Millisecond)
	require.Error(t, err)
}
