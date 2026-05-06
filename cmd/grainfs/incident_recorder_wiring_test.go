package main

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/serveruntime"
)

func TestIncidentRecorderInterfacesReturnNilInterfacesForNilRecorder(t *testing.T) {
	clusterRecorder, scrubberRecorder := serveruntime.IncidentRecorderInterfaces(nil)

	require.Nil(t, clusterRecorder)
	require.Nil(t, scrubberRecorder)
}
