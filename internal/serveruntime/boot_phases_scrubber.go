package serveruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server"
	"github.com/gritive/GrainFS/internal/serveruntime/executioncluster"
	"github.com/gritive/GrainFS/internal/startuprecovery"
	"github.com/gritive/GrainFS/internal/volume"
)

// bootRecoveryAndScrubber wires the per-node startup recovery, the scrubber
// Director (with replication + EC sources), placement monitors, and the
// scrubber-aware healing emitter.
//
// Phase ordering invariant (mirrors PR 4 raft phases): MUST register
// metaRaft.FSM().SetOnScrubTrigger AND forwardReceiver.WithScrubSessionLookup
// BEFORE director.Start(ctx). If a future refactor moves Start ahead of either
// callback registration, the first FSM-triggered scrub event is dropped.
//
// Inputs:  state.srv, state.receiptWiring, state.cfg.DataDir,
//
//	state.cfg.ScrubInterval, state.dgMgr, state.clusterRouter,
//	state.distBackend, state.shardSvc, state.backend, state.nodeID,
//	state.metaRaft, state.forwardReceiver, state.clusterCoord,
//	state.adminDeps, state.incidentRecorder.
//
// Outputs: state.activeEmitter, state.scrubDirector.
func bootRecoveryAndScrubber(ctx context.Context, state *bootState) error {
	cfg := state.cfg
	srv := state.srv

	// receiptWiring.keyStore may be nil when heal-receipt is disabled — in that
	// case NewReceiptTrackingEmitter degrades to a pass-through.
	var activeEmitter scrubber.Emitter = srv.HealEmitter()
	if state.receiptWiring != nil && state.receiptWiring.Store() != nil {
		rte := server.NewReceiptTrackingEmitter(srv.HealEmitter(), state.receiptWiring.Store(), state.receiptWiring.KeyStore())
		// Original: `defer rte.Close()` at Run() exit. Convert to AddCleanup
		// so we close at Run() exit (LIFO with everything else).
		state.AddCleanup(func() { rte.Close() })
		activeEmitter = rte
	}
	state.activeEmitter = activeEmitter

	// Phase 16 Week 3: cluster mode also needs startup recovery for the
	// node's local data dir (per-node multipart parts + .tmp leftovers).
	if rec, err := startuprecovery.Run(ctx, cfg.DataDir, srv.Operations(), activeEmitter); err != nil && !errors.Is(err, context.Canceled) {
		log.Warn().Err(err).Msg("startup recovery failed")
	} else if rec.OrphanTmpRemoved+rec.OrphanMultipartRemoved+len(rec.Errors) > 0 {
		log.Info().
			Int("orphan_tmp", rec.OrphanTmpRemoved).
			Int("orphan_multipart", rec.OrphanMultipartRemoved).
			Int("errors", len(rec.Errors)).Msg("startup recovery summary")
	}

	clusterIncidentRecorder, scrubberIncidentRecorder := IncidentRecorderInterfaces(state.incidentRecorder)

	// All three plumbings (walk, opener, repair) route through the local
	// data-group that owns the bucket — single-node serve still sits inside
	// a multi-raft group structure, so the volume bucket's files live under
	// {dataDir}/groups/<gid>/ rather than the bare distBackend root.
	dgMgr := state.dgMgr
	clusterRouter := state.clusterRouter
	groupBackendForBucket := func(bucket string) *cluster.DistributedBackend {
		dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
		if !ok || dg == nil || dg.Backend() == nil {
			return nil
		}
		return dg.Backend().DistributedBackend
	}
	opener := scrubber.LocalOpener(func(bucket, key string) (io.ReadCloser, error) {
		gb := groupBackendForBucket(bucket)
		if gb == nil {
			return nil, fmt.Errorf("scrub opener: no local group for %s", bucket)
		}
		return gb.OpenLocalReplica(bucket, key)
	})
	repairer := scrubber.ReplicaRepairer(ReplicaRepairerFunc(func(rctx context.Context, bucket, key string) error {
		gb := groupBackendForBucket(bucket)
		if gb == nil {
			return fmt.Errorf("scrub repair: no local group for %s", bucket)
		}
		return gb.RepairReplica(rctx, bucket, key)
	}))
	replSource := scrubber.NewReplicationObjectSource("replication", volume.VolumeBucketName, volume.MetaPrefix, state.backend)
	replVerifier := scrubber.NewReplicationVerifier(opener, repairer)

	director := scrubber.NewDirector(scrubber.DirectorOpts{
		Incident:  scrubberIncidentRecorder,
		QueueSize: 64,
		NodeID:    state.nodeID,
	})
	director.Register("replication", replSource, replVerifier)

	// PR4: EC scrub source via per-bucket group resolver.
	ecResolver := func(bucket string) (scrubber.Scrubbable, bool) {
		dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
		if !ok || dg == nil {
			return nil, false
		}
		gb := dg.Backend()
		if gb == nil {
			return nil, false
		}
		return gb, true
	}
	ecSource := scrubber.NewECScrubSource(ecResolver, state.nodeID)
	ecScrubVerifier := scrubber.NewShardVerifier(state.distBackend)
	ecScrubLimiter := rate.NewLimiter(rate.Limit(100), 100)
	ecVerifier := scrubber.NewECScrubVerifier(state.distBackend, ecScrubVerifier, ecScrubLimiter, activeEmitter, state.nodeID, ecSource)
	director.Register("ec", ecSource, ecVerifier)

	// PR4: cluster-wide scrub trigger via meta-raft. Each node's MetaFSM fires
	// onScrubTrigger when MetaScrubTriggerCmd applies; Director.ApplyFromFSM
	// creates a session for the same SessionID. MUST register BEFORE Start.
	if state.metaRaft != nil {
		state.metaRaft.FSM().SetOnScrubTrigger(func(entry scrubber.ScrubTriggerEntry) {
			director.ApplyFromFSM(entry)
		})
	}
	state.forwardReceiver.WithScrubSessionLookup(director)

	director.Start(ctx)
	state.scrubDirector = director
	state.adminDeps.Director = director
	state.adminDeps.ScrubAggregator = NewScrubAggregatorAdapter(state.clusterCoord)
	if state.metaRaft != nil {
		scrubProposer := NewScrubProposerAdapter(state.metaRaft, director, state.nodeID)
		state.adminDeps.ScrubProposer = scrubProposer
		exec := executioncluster.NewExecutor(
			NewScrubExecutionBackend(scrubProposer),
			executioncluster.WithMaxAttempts(3),
			executioncluster.WithRetryBackoff(50*time.Millisecond),
			executioncluster.WithMetrics(executioncluster.NewPrometheusMetrics()),
		)
		state.adminDeps.Execution = exec
		state.AddCleanup(func() { _ = exec.Close(context.Background()) })
	}

	if cfg.ScrubInterval > 0 {
		sc := scrubber.New(state.distBackend, cfg.ScrubInterval)
		sc.SetEmitter(activeEmitter)
		sc.RegisterSource("replication", replSource, replVerifier)
		sc.Start(ctx)

		placementMonitors := NewPlacementMonitorRegistry()
		shardSvc := state.shardSvc
		receiptWiring := state.receiptWiring
		startPlacementMonitor := func(monitorCtx context.Context, dg *cluster.DataGroup) {
			gb := dg.Backend()
			if clusterIncidentRecorder != nil {
				gb.SetIncidentRecorder(clusterIncidentRecorder)
			}
			placementMonitor := cluster.NewShardPlacementMonitor(gb.FSMRef(), gb, shardSvc, gb.NodeID(), cfg.ScrubInterval)
			splitShardKey := func(shardKey string) (string, string) {
				objectKey, versionID := shardKey, ""
				if i := strings.LastIndexByte(shardKey, '/'); i >= 0 {
					objectKey, versionID = shardKey[:i], shardKey[i+1:]
				}
				return objectKey, versionID
			}
			placementMonitor.SetOnMissing(func(bucket, shardKey string, shardIdx int) {
				objectKey, versionID := splitShardKey(shardKey)
				correlationID := uuid.Must(uuid.NewV7()).String()
				receiptID := "rcpt-" + correlationID
				repairReq := cluster.IncidentRepairRequest{
					Bucket:        bucket,
					Key:           objectKey,
					VersionID:     versionID,
					ShardIdx:      shardIdx,
					Recorder:      clusterIncidentRecorder,
					CorrelationID: correlationID,
				}
				if err := gb.RepairShardLocalWithIncident(monitorCtx, repairReq); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", bucket).Str("key", shardKey).Int("shard", shardIdx).Err(err).Msg("placement monitor repair failed")
				} else if receiptWiring != nil && receiptWiring.Store() != nil && receiptWiring.KeyStore() != nil {
					r := &receipt.HealReceipt{
						ReceiptID:     receiptID,
						Timestamp:     time.Now().UTC(),
						Object:        receipt.ObjectRef{Bucket: bucket, Key: objectKey, VersionID: versionID},
						ShardsLost:    []int32{int32(shardIdx)},
						ShardsRebuilt: []int32{int32(shardIdx)},
						EventIDs:      []string{correlationID},
						CorrelationID: correlationID,
					}
					if err := receipt.Sign(r, receiptWiring.KeyStore()); err != nil {
						log.Warn().Str("correlation_id", correlationID).Err(err).Msg("placement monitor receipt sign failed")
					} else if err := receiptWiring.Store().Put(r); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor receipt store failed")
					} else if err := receiptWiring.Store().Flush(); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor receipt flush failed")
					} else if err := gb.RecordRepairReceiptSigned(context.Background(), repairReq, receiptID); err != nil {
						log.Warn().Str("correlation_id", correlationID).Str("receipt_id", receiptID).Err(err).Msg("placement monitor incident proof update failed")
					}
				}
			})
			placementMonitor.SetOnCorrupt(func(bucket, shardKey string, shardIdx int, readErr error) {
				objectKey, versionID := splitShardKey(shardKey)
				if err := gb.QuarantineCorruptShardLocal(bucket, objectKey, versionID, shardIdx, readErr.Error()); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", bucket).Str("key", shardKey).Int("shard", shardIdx).Err(err).Msg("placement monitor quarantine failed")
				}
			})
			go placementMonitor.Start(monitorCtx)
		}
		refreshPlacementMonitors := func() {
			placementMonitors.Refresh(ctx, dgMgr.All(), startPlacementMonitor)
		}
		refreshPlacementMonitors()
		go func() {
			ticker := time.NewTicker(cfg.ScrubInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					refreshPlacementMonitors()
				}
			}
		}()
		log.Info().Dur("interval", cfg.ScrubInterval).Msg("cluster scrubber started")
	}
	return nil
}
