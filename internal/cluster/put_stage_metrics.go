package cluster

import (
	"time"

	"github.com/gritive/GrainFS/internal/metrics"
)

func observePutStage(path, stage string, start time.Time) {
	metrics.ObjectPutStageDuration.WithLabelValues(path, stage).Observe(time.Since(start).Seconds())
}
