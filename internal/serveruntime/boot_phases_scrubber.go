package serveruntime

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog/log"
	"golang.org/x/time/rate"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/receipt"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/server/receiptsvc"
	"github.com/gritive/GrainFS/internal/startuprecovery"
)

// redundancyUpgradeMax decides whether the EC-redundancy-upgrade sweep should
// be enabled on the scrubber and, if so, the per-cycle relocation cap. A
// non-positive configured max falls back to the default of 8.
func redundancyUpgradeMax(cfg Config) (enabled bool, max int) {
	if !cfg.ECRedundancyUpgrade {
		return false, 0
	}
	max = cfg.ECRedundancyUpgradeMax
	if max <= 0 {
		max = 8
	}
	return true, max
}

// targetLogKey returns the log-friendly key for an EC shard scan target:
// ObjectKey for object-version targets, ShardKey for segment/coalesced targets.
func targetLogKey(t cluster.ECShardScanTarget) string {
	if t.Kind == cluster.ECShardObjectVersion {
		return t.ObjectKey
	}
	return t.ShardKey
}

// bootRecoveryAndScrubber wires the per-node startup recovery, the scrubber
// Director (with the EC scrub source), placement monitors, and the
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
		rte := receiptsvc.NewTrackingEmitter(srv.HealEmitter(), state.receiptWiring.Store(), state.receiptWiring.KeyStore())
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

	dgMgr := state.dgMgr
	clusterRouter := state.clusterRouter

	director := scrubber.NewDirector(scrubber.DirectorOpts{
		Incident:  scrubberIncidentRecorder,
		QueueSize: 64,
		NodeID:    state.nodeID,
	})

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
	}

	if cfg.ScrubInterval > 0 {
		// Plan 3.5: activate orphan-SEGMENT GC. Frozen-path source + orphan-log MUST
		// wire together (the scrubber's activation constraint). Multi-group: segments
		// live per-group under each group's b.root, so the sweep iterates the union of
		// every hosted group's buckets (SetOwningGroupBackendSource + the shared
		// SetHostedGroupBackendsSource below) and dispatches each per-bucket op to its
		// owning group's backend, gated per-bucket on that group's caught-up state
		// (only the caught-up leader of a group GCs its segments).
		var segGCOpts []scrubber.ScrubberOption
		if state.objSnapMgr != nil {
			state.distBackend.SetFrozenSegmentPathSource(state.objSnapMgr.AllFrozenSegmentPaths)
			segGCOpts = append(segGCOpts, scrubber.WithSegmentOrphanLog(state.distBackend.NewSegmentOrphanLog(), cfg.SegmentGCRetention))
			// EC full-object orphan-SHARD sweep: snapshot-pinned full-object
			// versions must stay known. Wired only when a snapshot Manager exists;
			// absent => allFrozenObjectVersionDirs fails closed and the sweep
			// never runs (no reclaim, but no risk of deleting a pinned version).
			state.distBackend.SetFrozenObjectVersionSource(state.objSnapMgr.AllFrozenObjectVersions)
		}
		// EC full-object orphan-shard sweep wiring. The shared ShardService
		// dataDirs commingle every local group's shards (plus shards the balancer
		// floated in from groups this node does not host — balancer.go is
		// group-blind by design, placement metadata tracks location). The sweep
		// judges each candidate against authoritative metadata: the union of every
		// locally-hosted group's versioned live-set + the group-agnostic
		// quorum-meta, and KEEPS any shard whose owning group is not locally hosted
		// (unjudgeable locally). All sources re-evaluate each sweep (membership
		// changes at runtime).
		state.distBackend.SetHostedGroupBackendsSource(func() []*cluster.DistributedBackend {
			groups := dgMgr.All()
			out := make([]*cluster.DistributedBackend, 0, len(groups))
			for _, g := range groups {
				gb := g.Backend()
				if gb == nil || gb.DistributedBackend == nil {
					continue
				}
				out = append(out, gb.DistributedBackend)
			}
			return out
		})
		state.distBackend.SetOwningGroupHostedChecker(func(bucket string) bool {
			dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
			if !ok || dg == nil {
				// Unknown owner → cannot judge → keep. With requireExplicit routing
				// an unassigned bucket lands here too, so its shards are never swept
				// (more conservative than #774; every CreateBucket assigns a group,
				// so steady-state reclaim is unaffected).
				return false
			}
			gb := dg.Backend()
			return gb != nil && gb.DistributedBackend != nil
		})
		// Also gates the per-version orphan-blob sweep
		// (orphan_quorum_meta_version_walker.go): WalkOrphanQuorumMetaVersions calls
		// orphanShardSweepAllowed() and reuses the same hosted-group/owning-group
		// sources wired above — no separate wiring needed.
		state.distBackend.SetOrphanShardSweepGate(func() bool { return true })
		// Orphan-SEGMENT sweep: resolve each bucket to its owning group's backend so
		// the per-bucket walk/scan/delete runs on that group's b.root subtree. nil
		// when the owner is not locally hosted (its segments aren't on this node).
		state.distBackend.SetOwningGroupBackendSource(func(bucket string) *cluster.DistributedBackend {
			dg, ok := dgMgr.GroupForBucket(bucket, clusterRouter)
			if !ok || dg == nil {
				return nil
			}
			gb := dg.Backend()
			if gb == nil {
				return nil
			}
			return gb.DistributedBackend
		})
		sc := scrubber.New(state.distBackend, cfg.ScrubInterval, segGCOpts...)
		sc.SetEmitter(activeEmitter)
		if enabled, max := redundancyUpgradeMax(cfg); enabled {
			sc.EnableRedundancyUpgrade(max, cfg.ECRedundancyUpgradeMinAge)
		}
		// Stale mpudone marker GC always runs with the scrubber (S4c-0 PR2 Task 5b):
		// the marker keyspace is unbounded without it, so this is unconditional.
		sc.EnableMultipartDoneSweep(256, 24*time.Hour)
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
			placementMonitor.SetOnMissing(func(target cluster.ECShardScanTarget, shardIdx int) {
				correlationID := uuid.Must(uuid.NewV7()).String()
				receiptID := "rcpt-" + correlationID
				repairReq := cluster.IncidentRepairRequest{
					Bucket:        target.Bucket,
					Key:           target.ObjectKey,
					VersionID:     target.VersionID,
					ShardIdx:      shardIdx,
					Recorder:      clusterIncidentRecorder,
					CorrelationID: correlationID,
				}
				switch target.Kind {
				case cluster.ECShardObjectVersion:
					// plain object-version shard: no ShardKey/Placement needed
				case cluster.ECShardSegment, cluster.ECShardCoalesced:
					// Re-verify the shard is still referenced in the current meta
					// before repairing: coalesce/delete/GC between the monitor's
					// scan and this deferred callback can de-reference it, and
					// reconstructing an orphan shard would just fight GC.
					if !gb.ShardTargetStillReferenced(monitorCtx, target) {
						log.Info().Str("group", dg.ID()).Str("bucket", target.Bucket).Str("key", target.ShardKey).Int("shard", shardIdx).Msg("skipping repair of de-referenced shard")
						return
					}
					repairReq.ShardKey = target.ShardKey
					repairReq.Placement = target.Placement
				}
				if err := gb.RepairShardLocalWithIncident(monitorCtx, repairReq); err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", target.Bucket).Str("key", targetLogKey(target)).Int("shard", shardIdx).Err(err).Msg("placement monitor repair failed")
				} else if receiptWiring != nil && receiptWiring.Store() != nil && receiptWiring.KeyStore() != nil {
					r := &receipt.HealReceipt{
						ReceiptID:     receiptID,
						Timestamp:     time.Now().UTC(),
						Object:        receipt.ObjectRef{Bucket: target.Bucket, Key: target.ObjectKey, VersionID: target.VersionID},
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
			placementMonitor.SetOnCorrupt(func(target cluster.ECShardScanTarget, shardIdx int, readErr error) {
				var err error
				switch target.Kind {
				case cluster.ECShardObjectVersion:
					err = gb.QuarantineCorruptShardLocal(target.Bucket, target.ObjectKey, target.VersionID, shardIdx, readErr.Error())
				case cluster.ECShardSegment, cluster.ECShardCoalesced:
					err = gb.QuarantineCorruptShardLocalAtShardKey(target, shardIdx, readErr.Error())
				}
				if err != nil {
					log.Warn().Str("group", dg.ID()).Str("bucket", target.Bucket).Str("key", targetLogKey(target)).Int("shard", shardIdx).Err(err).Msg("placement monitor quarantine failed")
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
