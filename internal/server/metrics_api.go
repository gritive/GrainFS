package server

import (
	"context"
	"strconv"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/storage"
)

func (s *Server) metricsMiddleware() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)
		duration := time.Since(start).Seconds()
		method := string(c.Method())
		status := strconv.Itoa(c.Response.StatusCode())
		metrics.HTTPRequestsTotal.WithLabelValues(method, status).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method).Observe(duration)
	}
}

// initMetrics scans existing buckets and objects to set initial gauge values.
func (s *Server) initMetrics() {
	s.initStorageMetrics(context.Background())
}

func (s *Server) initStorageMetrics(ctx context.Context) {
	initStorageMetrics(ctx, s.ops)
}

func initStorageMetrics(ctx context.Context, ops *storage.Operations) {
	if ops == nil {
		return
	}
	buckets, err := ops.ListBuckets(ctx)
	if err != nil {
		return
	}
	metrics.BucketsTotal.Set(float64(len(buckets)))

	var totalObjects int
	var totalBytes int64
	for _, b := range buckets {
		objects, err := ops.ListObjects(ctx, b, "", 1000000)
		if err != nil {
			continue
		}
		totalObjects += len(objects)
		for _, obj := range objects {
			totalBytes += obj.Size
		}
	}
	metrics.ObjectsTotal.Set(float64(totalObjects))
	metrics.StorageBytesTotal.Set(float64(totalBytes))
}
