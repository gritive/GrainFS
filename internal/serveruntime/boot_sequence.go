package serveruntime

import (
	"context"
	"fmt"
)

// bootPhase is one ordered step of Run's boot sequence. name is the operator-
// facing label that prefixes any error the phase returns ("boot phase <name>:
// ...") and the identity the ordering/completeness tests assert against. run is
// the uniform phase signature; phases that do not need ctx ignore it.
//
// Order-as-data: the load-bearing linear boot order lives in bootSequence()
// below as an explicit slice — NOT as a source-text layout asserted by grep,
// and NOT as a DAG whose edges hide the order. Run() executes the slice in
// order with fail-fast + LIFO cleanup (state.Cleanup), exactly as the hand-
// inlined sequence did.
type bootPhase struct {
	name string
	run  func(ctx context.Context, state *bootState) error
}

// bootSequence is the authoritative, ordered list of boot phases Run() runs.
//
// Invariants encoded here (each previously enforced by a source-grep guard or
// inline comment, now data + name-based tests in boot_phase_split_test.go):
//   - Raft callbacks (DataGroupRouter, RotationAndAdminAPI, KEKRotationLeader)
//     register BEFORE bootMetaRaftStart fires the meta-raft apply loop.
//   - bootDataRaftNode constructs + early-Starts the data-raft node BEFORE the
//     invite-join Phase-2 inside bootWALAndForwardersPart1 (5-node EC join
//     deadlock avoidance, see boot_phases_raft.go bootDataRaftNode comments).
//   - bootWALAndForwardersPart1 populates the DEK keeper BEFORE
//     bootWaitDEKReady gates on it.
//   - bootShardService (constructs the data encryptor) runs AFTER
//     bootWaitDEKReady so it sees a populated keeper.
//   - wireRewrapLanes runs AFTER bootBackendWrap (needs distBackend +
//     packedBackend) and before bootRegisterForwardHandlers.
//   - bootRegisterForwardHandlers runs AFTER bootShardService so racing
//     forwarded RPCs see a clean not-ready error, BEFORE bootBalancerAndGossip.
//   - bootTLSPostureGate / bootPhase0Banner run AFTER bootHTTPServerAndAdmin
//     and BEFORE bootDegradedAndServices (which starts the listener).
//   - bootGreenfieldDEKBoundary runs AFTER bootRecoveryAndScrubber and BEFORE
//     bootDegradedAndServices.
//   - bootSelfRegisterMember runs LAST (after Phase-2 membership promotion).
//
// bootShutdownDrain is intentionally NOT a phase: it returns no error and
// blocks on ctx.Done(); Run() calls it after the loop completes.
func bootSequence() []bootPhase {
	return []bootPhase{
		// config + storage open.
		{"bootValidateConfig", func(_ context.Context, s *bootState) error { return bootValidateConfig(s) }},
		{"bootAutoMigrate", func(_ context.Context, s *bootState) error { return bootAutoMigrate(s) }},
		{"bootOpenMetaDB", func(_ context.Context, s *bootState) error { return bootOpenMetaDB(s) }},
		{"bootValidateTimings", func(_ context.Context, s *bootState) error { return bootValidateTimings(s) }},
		{"bootOpenSharedFSMDB", func(_ context.Context, s *bootState) error { return bootOpenSharedFSMDB(s) }},
		// bootRaftStoreKey returns bare errors; the "raft store key" domain
		// context lived at the former Run() call site, so preserve it here (the
		// loop adds the "boot phase ..." prefix on top).
		{"bootRaftStoreKey", func(_ context.Context, s *bootState) error {
			if err := bootRaftStoreKey(s); err != nil {
				return fmt.Errorf("raft store key: %w", err)
			}
			return nil
		}},

		// transport. groupRaftMux must exist BEFORE NewMetaTransport
		// (bootMetaRaftWiring) so the meta-raft transport auto-registers.
		{"bootClusterTransport", bootClusterTransport},
		{"bootGroupRaftMux", func(_ context.Context, s *bootState) error { return bootGroupRaftMux(s) }},

		// data-plane raft node: construct + RPC-bridge wire + early Start.
		{"bootDataRaftNode", bootDataRaftNode},

		// meta-raft callback registration BEFORE Start.
		{"bootMetaRaftWiring", func(_ context.Context, s *bootState) error { return bootMetaRaftWiring(s) }},
		{"bootDataGroupRouter", func(_ context.Context, s *bootState) error { return bootDataGroupRouter(s) }},
		{"bootRotationAndAdminAPI", func(_ context.Context, s *bootState) error { return bootRotationAndAdminAPI(s) }},
		{"bootKEKRotationLeader", func(_ context.Context, s *bootState) error { return bootKEKRotationLeader(s) }},
		{"bootMetaRaftStart", bootMetaRaftStart},
		{"bootGenesisDEKBootstrap", bootGenesisDEKBootstrap},

		// forwarders + DEK readiness gate.
		{"bootWALAndForwardersPart1", bootWALAndForwardersPart1},
		{"bootWaitDEKReady", bootWaitDEKReady},

		// storage runtime (runs after the DEK keeper is populated + gated).
		{"bootShardService", bootShardService},
		{"bootShardRoutes", func(_ context.Context, s *bootState) error { return bootShardRoutes(s) }},
		{"bootOwnedGroupsAndEC", bootOwnedGroupsAndEC},
		{"bootClusterCoordinatorRouting", func(_ context.Context, s *bootState) error { return bootClusterCoordinatorRouting(s) }},

		// snapshot + apply-loop, backend wrap, rewrap lanes, forward handlers.
		// bootSnapshotAndApplyLoop returned bare errors; preserve the former
		// Run() call-site context ("failed to initialize distributed storage")
		// so operators grepping for it still match (loop adds "boot phase ...").
		{"bootSnapshotAndApplyLoop", func(_ context.Context, s *bootState) error {
			if err := bootSnapshotAndApplyLoop(s); err != nil {
				return fmt.Errorf("failed to initialize distributed storage: %w", err)
			}
			return nil
		}},
		{"bootBackendWrap", bootBackendWrap},
		{"wireRewrapLanes", func(_ context.Context, s *bootState) error { wireRewrapLanes(s); return nil }},
		{"bootRegisterForwardHandlers", func(_ context.Context, s *bootState) error { return bootRegisterForwardHandlers(s) }},

		// services + posture gates + scrubber.
		{"bootBalancerAndGossip", bootBalancerAndGossip},
		{"bootSrvOptsAndReceipt", bootSrvOptsAndReceipt},
		{"bootHTTPServerAndAdmin", func(_ context.Context, s *bootState) error { return bootHTTPServerAndAdmin(s) }},
		{"bootTLSPostureGate", func(_ context.Context, s *bootState) error { return bootTLSPostureGate(s) }},
		{"bootPhase0Banner", func(_ context.Context, s *bootState) error { return bootPhase0Banner(s) }},
		{"bootRecoveryAndScrubber", bootRecoveryAndScrubber},
		{"bootGreenfieldDEKBoundary", bootGreenfieldDEKBoundary},
		{"bootDegradedAndServices", bootDegradedAndServices},
		{"bootSelfRegisterMember", bootSelfRegisterMember},
	}
}
