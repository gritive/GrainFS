package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIncidentRecorderInterfacesReturnNilInterfacesForNilRecorder(t *testing.T) {
	clusterRecorder, scrubberRecorder := incidentRecorderInterfaces(nil)

	require.Nil(t, clusterRecorder)
	require.Nil(t, scrubberRecorder)
}
