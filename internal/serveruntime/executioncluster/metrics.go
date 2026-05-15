package executioncluster

import "time"

type Metrics interface {
	RecordQueueDepth(int)
	RecordJobDuration(time.Duration)
	RecordRetry()
	RecordTimeout()
	RecordWorkerFailure()
	RecordAggregationFailure()
}

type NoopMetrics struct{}

func (NoopMetrics) RecordQueueDepth(int)            {}
func (NoopMetrics) RecordJobDuration(time.Duration) {}
func (NoopMetrics) RecordRetry()                    {}
func (NoopMetrics) RecordTimeout()                  {}
func (NoopMetrics) RecordWorkerFailure()            {}
func (NoopMetrics) RecordAggregationFailure()       {}
