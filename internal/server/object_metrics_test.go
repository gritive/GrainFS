package server

import (
	"testing"

	"github.com/gritive/GrainFS/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestObjectWriteMetricDeltaCreatesNewObject(t *testing.T) {
	delta := objectWriteMetricDelta(storage.PreviousObject{}, 42)

	require.Equal(t, objectMetricDelta{objects: 1, bytes: 42}, delta)
}

func TestObjectWriteMetricDeltaReplacesExistingObject(t *testing.T) {
	delta := objectWriteMetricDelta(storage.PreviousObject{Exists: true, Size: 12}, 42)

	require.Equal(t, objectMetricDelta{objects: 0, bytes: 30}, delta)
}

func TestObjectDeleteMetricDeltaDeletesOnlyExistingObject(t *testing.T) {
	require.Equal(t, objectMetricDelta{objects: -1, bytes: -12}, objectDeleteMetricDelta(storage.PreviousObject{Exists: true, Size: 12}))
	require.Equal(t, objectMetricDelta{}, objectDeleteMetricDelta(storage.PreviousObject{}))
}
