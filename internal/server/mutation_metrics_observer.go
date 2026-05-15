package server

import (
	"context"

	"github.com/gritive/GrainFS/internal/storage"
)

// metricsObserver synchronously updates Prometheus counters for object
// write/delete/copy. Bucket lifecycle has no metric (counts are derived
// from existing bucket-level gauges populated elsewhere).
type metricsObserver struct{}

func newMetricsObserver() *metricsObserver { return &metricsObserver{} }

func (m *metricsObserver) OnObjectWrite(_ context.Context, _, _ string, r *storage.PutObjectResult) {
	if r == nil {
		return
	}
	recordObjectWriteMetrics(r.Previous, r.Object.Size)
}

func (m *metricsObserver) OnObjectDelete(_ context.Context, _, _ string, r *storage.DeleteObjectResult) {
	if r == nil {
		return
	}
	recordObjectDeleteMetrics(r.Previous)
}

func (m *metricsObserver) OnObjectCopy(_ context.Context, _, _, _, _ string, r *storage.CopyObjectResult) {
	if r == nil {
		return
	}
	recordObjectWriteMetrics(r.Previous, r.Object.Size)
}

func (m *metricsObserver) OnBucketCreate(context.Context, string) {}
func (m *metricsObserver) OnBucketDelete(context.Context, string) {}
