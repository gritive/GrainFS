package serveruntime

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/badgerrole"
	"github.com/gritive/GrainFS/internal/cluster"
)

type incidentRecorderSinkStub struct {
	recorder cluster.IncidentRecorder
}

func (s *incidentRecorderSinkStub) SetIncidentRecorder(recorder cluster.IncidentRecorder) {
	s.recorder = recorder
}

func TestSetupIncidentRuntimeWiresRecorderStoreAndIdempotentClose(t *testing.T) {
	sink := &incidentRecorderSinkStub{}

	runtime, err := SetupIncidentRuntime(context.Background(), IncidentRuntimeOptions{
		RoleRegistry:          badgerrole.DefaultRegistry(),
		DataDir:               t.TempDir(),
		NodeID:                "node-1",
		IncidentRecorderSink:  sink,
		FDWatchEnabled:        false,
		GoroutineWatchEnabled: false,
		VlogWatchEnabled:      false,
	})

	require.NoError(t, err)
	require.NotNil(t, runtime.Recorder)
	require.NotNil(t, runtime.ClusterRecorder)
	require.NotNil(t, runtime.ScrubberRecorder)
	require.NotEmpty(t, runtime.ServerOptions)
	require.Same(t, runtime.Recorder, sink.recorder)
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
}

func TestSetupIncidentRuntimeDisablesOptionalIncidentRole(t *testing.T) {
	dataFile := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(dataFile, []byte("x"), 0o644))

	runtime, err := SetupIncidentRuntime(context.Background(), IncidentRuntimeOptions{
		RoleRegistry:          badgerrole.DefaultRegistry(),
		DataDir:               dataFile,
		NodeID:                "node-1",
		FDWatchEnabled:        true,
		GoroutineWatchEnabled: true,
		VlogWatchEnabled:      true,
	})

	require.NoError(t, err)
	require.Nil(t, runtime.Recorder)
	require.Nil(t, runtime.ClusterRecorder)
	require.Nil(t, runtime.ScrubberRecorder)
	require.Empty(t, runtime.ServerOptions)
	require.NoError(t, runtime.Close())
	require.NoError(t, runtime.Close())
}

func TestSetupIncidentRuntimeReturnsHardRoleFailure(t *testing.T) {
	runtime, err := SetupIncidentRuntime(context.Background(), IncidentRuntimeOptions{
		RoleRegistry: badgerrole.Registry{},
		DataDir:      t.TempDir(),
		NodeID:       "node-1",
	})

	require.Error(t, err)
	require.Contains(t, err.Error(), "incident runtime")
	require.Nil(t, runtime.Recorder)
	require.Empty(t, runtime.ServerOptions)
	require.NoError(t, runtime.Close())
}
