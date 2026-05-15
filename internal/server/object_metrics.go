package server

import (
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

type objectMetricDelta struct {
	objects int64
	bytes   int64
}

func objectWriteMetricDelta(previous storage.PreviousObject, newSize int64) objectMetricDelta {
	if previous.Exists {
		return objectMetricDelta{bytes: -previous.Size + newSize}
	}
	return objectMetricDelta{objects: 1, bytes: newSize}
}

func objectDeleteMetricDelta(previous storage.PreviousObject) objectMetricDelta {
	if !previous.Exists {
		return objectMetricDelta{}
	}
	return objectMetricDelta{objects: -1, bytes: -previous.Size}
}

func applyObjectMetricDelta(delta objectMetricDelta) {
	if delta.objects != 0 {
		metrics.ObjectsTotal.Add(float64(delta.objects))
	}
	if delta.bytes != 0 {
		metrics.StorageBytesTotal.Add(float64(delta.bytes))
	}
}

func recordObjectWriteMetrics(previous storage.PreviousObject, newSize int64) {
	applyObjectMetricDelta(objectWriteMetricDelta(previous, newSize))
}

func recordObjectDeleteMetrics(previous storage.PreviousObject) {
	applyObjectMetricDelta(objectDeleteMetricDelta(previous))
}
