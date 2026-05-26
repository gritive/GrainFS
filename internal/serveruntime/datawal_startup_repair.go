package serveruntime

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/metrics"
	"github.com/gritive/GrainFS/internal/receipt"
)

type dataWALStartupRepairResult struct {
	Repaired bool
	Skipped  string
	Failed   string
}

type dataWALStartupRepairer interface {
	RepairDataWALStartupCandidate(context.Context, cluster.DataWALRepairCandidate) dataWALStartupRepairResult
}

type dataWALStartupRepairFunc func(context.Context, cluster.DataWALRepairCandidate) dataWALStartupRepairResult

func (f dataWALStartupRepairFunc) RepairDataWALStartupCandidate(ctx context.Context, candidate cluster.DataWALRepairCandidate) dataWALStartupRepairResult {
	return f(ctx, candidate)
}

func splitDataWALStartupRepairShardKey(shardKey string) (string, string) {
	objectKey, versionID := shardKey, ""
	if i := strings.LastIndexByte(shardKey, '/'); i >= 0 {
		objectKey, versionID = shardKey[:i], shardKey[i+1:]
	}
	return objectKey, versionID
}

func classifyDataWALStartupRepairFailure(err error) string {
	if errors.Is(err, context.Canceled) {
		return "context_canceled"
	}
	if err != nil && (strings.Contains(err.Error(), "other shards readable") || strings.Contains(err.Error(), "only ")) {
		return "insufficient_survivors"
	}
	return "repair_failed"
}

func runDataWALStartupRepairCandidates(ctx context.Context, candidates []cluster.DataWALRepairCandidate, repairer dataWALStartupRepairer) []dataWALStartupRepairResult {
	results := make([]dataWALStartupRepairResult, 0, len(candidates))
	for _, candidate := range candidates {
		if ctx.Err() != nil {
			break
		}
		result := repairer.RepairDataWALStartupCandidate(ctx, candidate)
		results = append(results, result)
		if result.Failed == "context_canceled" {
			break
		}
	}
	return results
}

type dataWALStartupRepairRuntime struct {
	dgMgr            *cluster.DataGroupManager
	router           *cluster.Router
	incidentRecorder cluster.IncidentRecorder
	receiptWiring    *HealReceiptWiring
}

func (r dataWALStartupRepairRuntime) RepairDataWALStartupCandidate(ctx context.Context, candidate cluster.DataWALRepairCandidate) dataWALStartupRepairResult {
	dg, ok := r.dgMgr.GroupForBucket(candidate.Bucket, r.router)
	if !ok || dg == nil {
		metrics.DataWALStartupRepairSkips.WithLabelValues("no_group").Inc()
		return dataWALStartupRepairResult{Skipped: "no_group"}
	}
	gb := dg.Backend()
	if gb == nil || gb.DistributedBackend == nil {
		metrics.DataWALStartupRepairSkips.WithLabelValues("no_backend").Inc()
		return dataWALStartupRepairResult{Skipped: "no_backend"}
	}
	objectKey, versionID := splitDataWALStartupRepairShardKey(candidate.ShardKey)
	rec, lookupErr := gb.FSMRef().LookupObjectPlacement(candidate.Bucket, objectKey, versionID)
	cfg := rec.ECConfigOrFallback(gb.CurrentECConfigForStartupRepair())
	skip := classifyDataWALStartupRepairPlacement(objectKey, candidate.ShardIdx, rec, cfg, lookupErr, gb.NodeID())
	if skip != "" {
		metrics.DataWALStartupRepairSkips.WithLabelValues(skip).Inc()
		return dataWALStartupRepairResult{Skipped: skip}
	}

	correlationID := uuid.Must(uuid.NewV7()).String()
	repairReq := cluster.IncidentRepairRequest{
		Bucket:        candidate.Bucket,
		Key:           objectKey,
		VersionID:     versionID,
		ShardIdx:      candidate.ShardIdx,
		Recorder:      r.incidentRecorder,
		CorrelationID: correlationID,
	}
	metrics.DataWALStartupRepairAttempts.Inc()
	if err := gb.RepairShardLocalWithIncident(ctx, repairReq); err != nil {
		reason := classifyDataWALStartupRepairFailure(err)
		metrics.DataWALStartupRepairFailures.WithLabelValues(reason).Inc()
		log.Warn().
			Err(err).
			Str("group", dg.ID()).
			Str("bucket", candidate.Bucket).
			Str("key", candidate.ShardKey).
			Int("shard", candidate.ShardIdx).
			Str("reason", reason).
			Msg("startup data WAL repair failed")
		return dataWALStartupRepairResult{Failed: reason}
	}
	metrics.DataWALStartupRepairSuccesses.Inc()
	writeDataWALStartupRepairReceipt(ctx, gb.DistributedBackend, r.receiptWiring, repairReq, correlationID)
	return dataWALStartupRepairResult{Repaired: true}
}

func classifyDataWALStartupRepairPlacement(objectKey string, shardIdx int, rec cluster.PlacementRecord, cfg cluster.ECConfig, lookupErr error, nodeID string) string {
	if objectKey == "" || shardIdx < 0 {
		return "invalid_shard_key"
	}
	if lookupErr != nil {
		return "placement_corrupt"
	}
	if len(rec.Nodes) == 0 {
		return "stale"
	}
	if cfg.NumShards() <= 0 || len(rec.Nodes) != cfg.NumShards() {
		return "placement_corrupt"
	}
	if shardIdx >= len(rec.Nodes) || rec.Nodes[shardIdx] != nodeID {
		return "not_local_owner"
	}
	return ""
}

func writeDataWALStartupRepairReceipt(ctx context.Context, gb *cluster.DistributedBackend, wiring *HealReceiptWiring, req cluster.IncidentRepairRequest, correlationID string) {
	if wiring == nil || wiring.Store() == nil || wiring.KeyStore() == nil {
		return
	}
	receiptID := "rcpt-" + correlationID
	r := &receipt.HealReceipt{
		ReceiptID:     receiptID,
		Timestamp:     time.Now().UTC(),
		Object:        receipt.ObjectRef{Bucket: req.Bucket, Key: req.Key, VersionID: req.VersionID},
		ShardsLost:    []int32{int32(req.ShardIdx)},
		ShardsRebuilt: []int32{int32(req.ShardIdx)},
		EventIDs:      []string{correlationID},
		CorrelationID: correlationID,
	}
	if err := receipt.Sign(r, wiring.KeyStore()); err != nil {
		log.Warn().Str("correlation_id", correlationID).Err(err).Msg("startup data WAL repair receipt sign failed")
		return
	}
	if err := wiring.Store().Put(r); err != nil {
		log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("startup data WAL repair receipt store failed")
		return
	}
	if err := wiring.Store().Flush(); err != nil {
		log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("startup data WAL repair receipt flush failed")
		return
	}
	if err := gb.RecordRepairReceiptSigned(ctx, req, receiptID); err != nil {
		log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("startup data WAL repair incident proof update failed")
	}
}

func startDataWALStartupRepairWorker(ctx context.Context, state *bootState) {
	if state.dataWALRepairCollector == nil {
		return
	}
	candidates := state.dataWALRepairCollector.Candidates()
	if len(candidates) == 0 {
		return
	}
	for _, candidate := range candidates {
		metrics.DataWALStartupRepairCandidates.WithLabelValues(string(candidate.Reason)).Inc()
	}
	clusterIncidentRecorder, _ := IncidentRecorderInterfaces(state.incidentRecorder)
	repairer := dataWALStartupRepairRuntime{
		dgMgr:            state.dgMgr,
		router:           state.clusterRouter,
		incidentRecorder: clusterIncidentRecorder,
		receiptWiring:    state.receiptWiring,
	}
	// Fire-and-forget: the worker is intentionally untracked and non-blocking.
	// Serving must never wait for repair to complete. If the process terminates
	// mid-receipt, the candidate is simply re-discovered on the next startup WAL
	// scan — repair is idempotent (RepairShardLocalWithIncident verifies the
	// local target before/after reconstruction).
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				log.Error().Interface("panic", rec).Msg("startup data WAL repair worker panicked; aborting (candidates re-discovered on next startup scan)")
			}
		}()
		log.Info().Int("candidates", len(candidates)).Msg("startup data WAL repair worker started")
		results := runDataWALStartupRepairCandidates(ctx, candidates, repairer)
		var repaired, skipped, failed int
		for _, result := range results {
			if result.Repaired {
				repaired++
			}
			if result.Skipped != "" {
				skipped++
			}
			if result.Failed != "" {
				failed++
			}
		}
		log.Info().
			Int("candidates", len(candidates)).
			Int("repaired", repaired).
			Int("skipped", skipped).
			Int("failed", failed).
			Msg("startup data WAL repair worker finished")
	}()
}
