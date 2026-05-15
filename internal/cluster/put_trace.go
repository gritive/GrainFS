package cluster

import (
	"context"
	"encoding/json"
	"os"
	"sync"
	"time"
)

type PutTraceStage string

const (
	PutTraceStageHTTPPutTotal            PutTraceStage = "http_put_total"
	PutTraceStageHTTPPutPrepare          PutTraceStage = "http_put_prepare"
	PutTraceStageHTTPPutBackend          PutTraceStage = "http_put_backend"
	PutTraceStageHTTPPutMutation         PutTraceStage = "http_put_mutation"
	PutTraceStageHTTPPutResponse         PutTraceStage = "http_put_response"
	PutTraceStageRouteWrite              PutTraceStage = "route_write"
	PutTraceStageForwardResolveLeader    PutTraceStage = "forward_resolve_leader"
	PutTraceStageForwardSendFrame        PutTraceStage = "forward_send_frame"
	PutTraceStageForwardSendStream       PutTraceStage = "forward_send_stream"
	PutTraceStageForwardNotLeaderRetry   PutTraceStage = "forward_not_leader_retry"
	PutTraceStageForwardReceiverDispatch PutTraceStage = "forward_receiver_dispatch"
	PutTraceStageReceiverBackendPut      PutTraceStage = "receiver_backend_put"
	PutTraceStageShardWriteLocal         PutTraceStage = "shard_write_local"
	PutTraceStageShardWriteRemote        PutTraceStage = "shard_write_remote"
	PutTraceStageDataRaftProposeMeta     PutTraceStage = "data_raft_propose_meta"
	PutTraceStageMetaIndexPropose        PutTraceStage = "meta_index_propose"
)

type PutTraceIngress string

const (
	PutTraceIngressLocalLeader        PutTraceIngress = "local_leader"
	PutTraceIngressForwardedNonLeader PutTraceIngress = "forwarded_non_leader"
	PutTraceIngressReceiver           PutTraceIngress = "receiver"
)

type PutTraceSizeClass string

const (
	PutTraceSizeSmall   PutTraceSizeClass = "small"
	PutTraceSizeLarge   PutTraceSizeClass = "large"
	PutTraceSizeUnknown PutTraceSizeClass = "unknown"
)

type PutTraceForwardMode string

const (
	PutTraceForwardNone   PutTraceForwardMode = "none"
	PutTraceForwardFrame  PutTraceForwardMode = "frame"
	PutTraceForwardStream PutTraceForwardMode = "stream"
)

type PutTraceRequest struct {
	Bucket      string
	Key         string
	GroupID     string
	Ingress     PutTraceIngress
	SizeClass   PutTraceSizeClass
	ForwardMode PutTraceForwardMode
}

type PutTraceStageFields struct {
	Bytes            int64
	ForwardAttempts  int
	LeaderHintUsed   bool
	LeaderHintStale  bool
	NotLeaderRetries int
	ShardIndex       int
	ShardTarget      string
	ShardTargetClass string
	MetaProposeSite  string
	MetaProposeCount int
	Error            string
}

type PutTraceEvent struct {
	TS               string              `json:"ts"`
	NodeID           string              `json:"node_id,omitempty"`
	PID              int                 `json:"pid"`
	Bucket           string              `json:"bucket"`
	Key              string              `json:"key"`
	GroupID          string              `json:"group_id,omitempty"`
	Ingress          PutTraceIngress     `json:"ingress"`
	SizeClass        PutTraceSizeClass   `json:"size_class"`
	ForwardMode      PutTraceForwardMode `json:"forward_mode"`
	Stage            PutTraceStage       `json:"stage"`
	DurationMicros   int64               `json:"duration_micros"`
	Bytes            int64               `json:"bytes,omitempty"`
	ForwardAttempts  int                 `json:"forward_attempts,omitempty"`
	LeaderHintUsed   bool                `json:"leader_hint_used,omitempty"`
	LeaderHintStale  bool                `json:"leader_hint_stale,omitempty"`
	NotLeaderRetries int                 `json:"not_leader_retries,omitempty"`
	ShardIndex       int                 `json:"shard_index,omitempty"`
	ShardTarget      string              `json:"shard_target,omitempty"`
	ShardTargetClass string              `json:"shard_target_class,omitempty"`
	MetaProposeSite  string              `json:"meta_propose_site,omitempty"`
	MetaProposeCount int                 `json:"meta_propose_count,omitempty"`
	Error            string              `json:"error,omitempty"`
}

type putTraceContextKey struct{}

type putTraceSink struct {
	mu     sync.Mutex
	file   *os.File
	nodeID string
	pid    int
}

var globalPutTraceSink = openPutTraceSinkFromEnv()

func openPutTraceSinkFromEnv() *putTraceSink {
	path := os.Getenv("GRAINFS_PUT_TRACE_FILE")
	if path == "" {
		return nil
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil
	}
	return &putTraceSink{
		file:   f,
		nodeID: os.Getenv("GRAINFS_NODE_ID"),
		pid:    os.Getpid(),
	}
}

func reloadPutTraceSinkForTest() {
	if globalPutTraceSink != nil && globalPutTraceSink.file != nil {
		_ = globalPutTraceSink.file.Close()
	}
	globalPutTraceSink = openPutTraceSinkFromEnv()
}

func putTraceEnabled() bool {
	return globalPutTraceSink != nil
}

func ContextWithPutTrace(ctx context.Context, req PutTraceRequest) context.Context {
	if req.SizeClass == "" {
		req.SizeClass = PutTraceSizeUnknown
	}
	if req.ForwardMode == "" {
		req.ForwardMode = PutTraceForwardNone
	}
	return context.WithValue(ctx, putTraceContextKey{}, req)
}

func PutTraceRequestFromContext(ctx context.Context) (PutTraceRequest, bool) {
	req, ok := ctx.Value(putTraceContextKey{}).(PutTraceRequest)
	return req, ok
}

func StartPutTraceStage(ctx context.Context, stage PutTraceStage) func(PutTraceStageFields) {
	start := time.Now()
	return func(fields PutTraceStageFields) {
		ObservePutTraceStage(ctx, stage, start, fields)
	}
}

func ObservePutTraceStage(ctx context.Context, stage PutTraceStage, start time.Time, fields PutTraceStageFields) {
	sink := globalPutTraceSink
	if sink == nil {
		return
	}
	req, ok := PutTraceRequestFromContext(ctx)
	if !ok {
		return
	}
	sink.write(PutTraceEvent{
		TS:               time.Now().UTC().Format(time.RFC3339Nano),
		NodeID:           sink.nodeID,
		PID:              sink.pid,
		Bucket:           req.Bucket,
		Key:              req.Key,
		GroupID:          req.GroupID,
		Ingress:          req.Ingress,
		SizeClass:        req.SizeClass,
		ForwardMode:      req.ForwardMode,
		Stage:            stage,
		DurationMicros:   time.Since(start).Microseconds(),
		Bytes:            fields.Bytes,
		ForwardAttempts:  fields.ForwardAttempts,
		LeaderHintUsed:   fields.LeaderHintUsed,
		LeaderHintStale:  fields.LeaderHintStale,
		NotLeaderRetries: fields.NotLeaderRetries,
		ShardIndex:       fields.ShardIndex,
		ShardTarget:      fields.ShardTarget,
		ShardTargetClass: fields.ShardTargetClass,
		MetaProposeSite:  fields.MetaProposeSite,
		MetaProposeCount: fields.MetaProposeCount,
		Error:            fields.Error,
	})
}

func (s *putTraceSink) write(ev PutTraceEvent) {
	data, err := json.Marshal(ev)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.file.Write(append(data, '\n'))
}

func putTraceSizeClass(n int64, maxSingleFrame int64) PutTraceSizeClass {
	if n > maxSingleFrame {
		return PutTraceSizeLarge
	}
	if n >= 0 {
		return PutTraceSizeSmall
	}
	return PutTraceSizeUnknown
}

func putTraceErrorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
