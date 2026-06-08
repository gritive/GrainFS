package serveruntime

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/transport"
)

// buildObjectIndexShards returns the object-index shard slice that the façade
// (ObjectIndexShardSet) wraps. At the default IndexGroupCount==1 it returns the
// single meta-FSM shard — Reader/Lister are the cluster's only meta-raft object
// index, Writer is the supplied forwarding proposer — BYTE-IDENTICAL to the
// pre-Slice-4b inline construction at both façade sites.
//
// At IndexGroupCount>1 the two early façade sites (bootWALAndForwardersPart1,
// bootClusterCoordinatorRouting) still get this meta-FSM shard as a TEMPORARY
// placeholder: they run BEFORE the index groups are seeded, so the real
// index-group façade cannot be assembled yet. bootIndexGroupsPostSeed (after
// seeding) REWIRES both consumers to the manager-backed shardset. Keeping the
// placeholder here lets boot proceed and keeps N=1 unperturbed.
func buildObjectIndexShards(state *bootState, indexProposer *cluster.ForwardingObjectIndexProposer) ([]cluster.ObjectIndexShard, error) {
	metaRaft := state.metaRaft
	// N=1 (and the pre-seed N>1 placeholder): the single meta-FSM shard.
	return []cluster.ObjectIndexShard{
		{Reader: metaRaft.FSM(), Writer: indexProposer, Lister: metaRaft.FSM()},
	}, nil
}

// bootIndexGroupsPostSeed assembles the N>1 sharded object-index façade AFTER the
// index groups have been seeded (genesis) or replayed (restart) into the meta-FSM
// and AFTER the coordinator + forward receiver exist (so it can rewire them). At
// the default IndexGroupCount<=1 it early-returns: the meta-FSM single-shard path
// (wired at the two façade sites) stays, so N=1 gets ZERO new phase work.
//
// Ordering (run.go): immediately after bootClusterCoordinatorRouting — the
// earliest point where BOTH rewire targets (forwardReceiver, clusterCoord) exist.
// The two façade sites run before seeding, so they cannot build the index-group
// façade; this phase is where the N>1 rewire happens.
//
// Steps for N>1:
//  1. WaitForIndexGroupCount(N) — confirm the seed/replay landed.
//  2. Build the sender (CallPooled dialer + leader-hint resolver), register the
//     leader-side receiver on shardSvc, register the onIndexGroupAdded callback
//     BEFORE the replay scan so live-replicated groups after the scan are also
//     instantiated (InstantiateAndStart is idempotent).
//  3. InstantiateAndStart over IndexGroups() (replay scan), wrap manager.Shards()
//     in an ObjectIndexShardSet, and rewire forwardReceiver + clusterCoord.
//  4. Register manager.Close() into shutdown.
func bootIndexGroupsPostSeed(ctx context.Context, state *bootState) error {
	count := normalizeIndexGroupCount(state.cfg.IndexGroupCount)
	if count <= 1 {
		return nil // N=1 default — meta-FSM single shard, no index groups
	}

	metaRaft := state.metaRaft
	peers := state.peers

	// 1. Confirm the seed (genesis) / replay (restart) landed before assembling.
	if err := WaitForIndexGroupCount(ctx, metaRaft.FSM(), count, 30*time.Second); err != nil {
		return fmt.Errorf("boot index groups: %w", err)
	}

	// 2a. Sender: follower→leader index-group proposal forward over the bounded
	// control pool (CallPooled — same reasoning as the data/meta forward dialers).
	clusterTransport := state.clusterTransport
	dialer := func(callCtx context.Context, peer string, payload []byte) ([]byte, error) {
		msg := &transport.Message{Type: transport.StreamIndexGroupProposeForward, Payload: payload}
		reply, err := clusterTransport.CallPooled(callCtx, peer, msg)
		if err != nil {
			return nil, err
		}
		return reply.Payload, nil
	}
	sender := cluster.NewIndexGroupProposeForwardSender(dialer).
		WithLeaderHintResolver(func(hint string) string {
			if addr, ok := cluster.ResolveNodeAddress(metaRaft.FSM(), hint); ok {
				return addr
			}
			return hint
		})

	mgr := cluster.NewIndexGroupManager()
	state.indexGroupMgr = mgr

	// 2b. Leader-side receiver: resolve forwarded group IDs to local groups.
	cluster.NewIndexGroupProposeForwardReceiver(mgr).Register(state.shardSvc)

	cfg := cluster.IndexGroupLifecycleConfig{
		NodeID:   state.nodeID,
		DataDir:  state.cfg.DataDir,
		KEKStore: state.kekStore,
		// Per-group raft RPCs ride the shared group-raft mux, exactly like data
		// groups (boot_phases_storage_runtime.go:608/623): the lifecycle wires
		// ForGroup(id) outbound + Register(id, node) inbound for each index group.
		GroupMux:         state.groupRaftMux,
		AddrBook:         metaRaft.FSM(),
		ElectionTimeout:  state.cfg.RaftElectionTimeout,
		HeartbeatTimeout: state.cfg.RaftHeartbeatInterval,
		SnapshotInterval: indexGroupSnapshotInterval,
	}
	// send is the follower→leader forward primitive (sender.Send). The manager
	// binds each group's hook over the group's OWN node LeaderID() at call-time —
	// serveruntime cannot name *indexGroup, so it supplies only this Send func and
	// the chicken-and-egg is resolved inside the manager. proposeOrForward only
	// invokes the hook when this node is NOT the group's leader.
	send := sender.Send

	// 2c. Register the callback BEFORE the replay scan so a group replicated/applied
	// AFTER the scan is also instantiated. InstantiateAndStart is idempotent, so the
	// scan + callback overlapping on the same ID is safe.
	//
	// The callback fires INSIDE the meta-FSM apply loop (applyPutIndexGroup), so it
	// must NOT block: index-group instantiation builds a raft node + Start, which is
	// slow. Dispatch it to a goroutine, shutdown-aware (mirrors the data-group
	// scheduleOwnedInstantiation at boot_phases_storage_runtime.go:633). The replay
	// scan below runs on THIS (boot) goroutine, so it stays synchronous.
	var (
		cbMu      sync.Mutex
		cbWG      sync.WaitGroup
		cbClosing bool
	)
	metaRaft.FSM().SetOnIndexGroupAdded(func(entry cluster.IndexGroupEntry) {
		cbMu.Lock()
		if cbClosing {
			cbMu.Unlock()
			return
		}
		cbWG.Add(1)
		cbMu.Unlock()
		go func() {
			defer cbWG.Done()
			if err := mgr.InstantiateAndStart(ctx, cfg, []cluster.IndexGroupEntry{entry}, send); err != nil {
				log.Error().Err(err).Str("group_id", entry.ID).Msg("index group: runtime instantiation failed")
			}
		}()
	})

	// 3. Replay scan: instantiate every already-known index group.
	if err := mgr.InstantiateAndStart(ctx, cfg, metaRaft.FSM().IndexGroups(), send); err != nil {
		return fmt.Errorf("boot index groups: instantiate: %w", err)
	}

	shardSet, err := cluster.NewObjectIndexShardSet(mgr.Shards())
	if err != nil {
		return fmt.Errorf("boot index groups: shard set: %w", err)
	}

	// Rewire both consumers from the meta-FSM placeholder to the sharded façade.
	// WithObjectIndex* are pointer-receiver mutators on the same instance; reassign
	// defensively so a future value-receiver change cannot silently no-op.
	state.forwardReceiver = state.forwardReceiver.WithObjectIndexProposer(shardSet)
	state.clusterCoord = state.clusterCoord.
		WithObjectIndexProposer(shardSet).
		WithObjectIndexReader(shardSet)

	// 4. Shutdown: unhook the callback, refuse any new dispatch, drain in-flight
	// instantiations (so a late-replication goroutine cannot register a started
	// group AFTER Close and leak it), then close every group + store.
	state.AddCleanup(func() {
		metaRaft.FSM().SetOnIndexGroupAdded(nil)
		cbMu.Lock()
		cbClosing = true
		cbMu.Unlock()
		cbWG.Wait()
		mgr.Close()
	})

	log.Info().Int("index_groups", count).Strs("peers", peers).Msg("sharded object index: N index groups wired")
	return nil
}

// indexGroupSnapshotInterval bounds the index-group raft log under load by firing
// a periodic apply-loop snapshot every N applied command entries. Mirrors the
// data-group compaction cadence; tuned conservatively.
const indexGroupSnapshotInterval = 4096
