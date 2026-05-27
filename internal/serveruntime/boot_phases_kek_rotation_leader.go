// Package serveruntime: production wiring of the KEK rotation leader
// (Task 11 commit 2).
//
// Composition:
//
//   - KEKRotationLeader receives FSM + raft submitter + peer probe + raft
//     config reader. The leader is then handed to MetaRaft so its
//     leadership watcher calls AcquireLeadership / LoseLeadership on every
//     transition (auto-aborting in-flight propose calls on step-down).
//   - PeerKEKProbe is the production impl from kek_peer_probe_production.go.
//     The local-self closures wrap statfs on the keystore directory and the
//     state.kekLeaseTracker count.
//   - The two QUIC handlers (StreamKEKDiskSpaceProbe,
//     StreamKEKLeaseSnapshotProbe) are registered on state.quicTransport so
//     the leader's fan-out reaches every voter.
//   - The KEK audit sink is opened at <dataDir>/audit/kek.log (0o600,
//     O_APPEND|O_CREATE|O_WRONLY) and set on the MetaFSM. Append-only file
//     handle outlives boot; closed on bootState.Cleanup.
//
// Ordering: this phase MUST run after bootMetaRaftWiring (state.metaRaft +
// state.kekStore + state.kekLeaseTracker populated) and BEFORE
// bootMetaRaftStart (the leadership watcher only consults the rotation leader
// set via SetKEKRotationLeader before Start fires).
package serveruntime

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/rs/zerolog/log"

	"github.com/gritive/GrainFS/internal/cluster"
	"github.com/gritive/GrainFS/internal/nodeconfig"
	"github.com/gritive/GrainFS/internal/transport"
)

// statfsDiskFreeBytes returns Bavail * Bsize for the filesystem backing dir.
// Mirrors internal/cluster's statfsDiskSpace; duplicated here only to avoid
// exporting that helper from the cluster package for one caller.
func statfsDiskFreeBytes(dir string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return 0, fmt.Errorf("statfs %q: %w", dir, err)
	}
	return uint64(stat.Bavail) * uint64(stat.Bsize), nil
}

// bootKEKRotationLeader constructs the rotation leader, wires it onto
// MetaRaft, registers the two peer probe RPC handlers, and opens the KEK
// audit log. Skipped (warn-only) when prerequisites are absent — keeps
// non-cluster test configurations boot-clean.
func bootKEKRotationLeader(state *bootState) error {
	if state.metaRaft == nil || state.quicTransport == nil || state.kekStore == nil {
		log.Debug().Msg("bootKEKRotationLeader: prerequisites unavailable; skipping (non-cluster mode)")
		return nil
	}

	cfg := nodeconfig.New(state.cfg.DataDir)
	keystoreDir := cfg.KEKDir()

	// KEK probe + attestation identity. The voter set the prune path validates
	// against comes from MetaRaft.Node().Configuration().Servers[].ID, which is
	// the raft ServerID (production wires RaftID = state.raftAddr, see
	// boot_phases_raft.go). The PeerKEKProbe self-shortcut and the probe handler
	// NodeIDs MUST use that SAME raft ServerID so (a) the self-call matches a
	// voter entry and (b) remote voters' attestation node_ids match the
	// leader-stamped voter_ids the FSM checks in validatePruneAttestation.
	// Using state.nodeID (the operator-facing --node-id) here would never match
	// the raft voter set → "missing probe response from voter <raft-addr>".
	raftServerID := state.metaRaft.Node().ID()

	// 1. Audit sink — append-only file at <dataDir>/audit/kek.log. 0o600 keeps
	//    it host-root-readable but not group/world. Errors surface as boot
	//    failures: a missing sink would silently drop KEK lifecycle audit
	//    records.
	auditDir := filepath.Join(state.cfg.DataDir, "audit")
	if err := os.MkdirAll(auditDir, 0o700); err != nil {
		return fmt.Errorf("bootKEKRotationLeader: mkdir audit dir: %w", err)
	}
	auditPath := filepath.Join(auditDir, "kek.log")
	auditFile, err := os.OpenFile(auditPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("bootKEKRotationLeader: open audit log %s: %w", auditPath, err)
	}
	state.metaRaft.FSM().SetAuditSink(auditFile)
	state.AddCleanup(func() { _ = auditFile.Close() })

	// 2. Peer probe RPC handlers. Register on the shared QUIC transport so a
	//    leader's GetKEKDiskSpace / GetKEKLeaseSnapshot reaches this node as a
	//    voter.
	state.quicTransport.Handle(transport.StreamKEKDiskSpaceProbe,
		cluster.NewKEKDiskSpaceHandler(raftServerID, keystoreDir, nil /* statfs default */).Handle)
	state.quicTransport.Handle(transport.StreamKEKLeaseSnapshotProbe,
		cluster.NewKEKLeaseSnapshotHandler(raftServerID, state.kekLeaseTracker, func() uint64 {
			return state.metaRaft.Node().CommittedIndex()
		}).Handle)

	// 3. Production PeerKEKProbe with self-shortcut. Self-call computes the
	//    disk-space + lease values directly (no wire codec roundtrip) so the
	//    leader's own readings are always available even if the QUIC
	//    transport is degraded mid-rotation.
	raftConfig := cluster.NewMetaRaftConfigReader(state.metaRaft)
	dialer := &cluster.QUICPeerProbeDialer{T: state.quicTransport}
	leaseTracker := state.kekLeaseTracker
	mr := state.metaRaft
	selfID := raftServerID
	probe := cluster.NewPeerKEKProbe(
		dialer,
		raftConfig,
		selfID,
		func() (cluster.KEKDiskSpaceResp, error) {
			free, err := statfsDiskFreeBytes(keystoreDir)
			if err != nil {
				return cluster.KEKDiskSpaceResp{}, err
			}
			return cluster.KEKDiskSpaceResp{
				NodeID:       selfID,
				KeystorePath: keystoreDir,
				FreeBytes:    free,
			}, nil
		},
		func(version uint32) (cluster.KEKLeaseSnapshotResp, error) {
			return cluster.KEKLeaseSnapshotResp{
				NodeID:                    selfID,
				LeaseCount:                leaseTracker.Count(version),
				ObservedAtRaftCommitIndex: mr.Node().CommittedIndex(),
			}, nil
		},
	)

	// 4. Construct the rotation leader. PeerProbe is required; the FSM and
	//    raft submitter come from state.metaRaft directly (its Propose
	//    method satisfies KEKRaftSubmitter).
	leader := cluster.NewKEKRotationLeader(cluster.KEKRotationLeaderConfig{
		FSM:              state.metaRaft.FSM(),
		Raft:             state.metaRaft,
		PeerProbe:        probe,
		RaftConfigReader: raftConfig,
	})
	state.kekRotationLeader = leader

	// 5. Hand the leader to MetaRaft so its leadership watcher (started by
	//    Start) calls AcquireLeadership / LoseLeadership on every raft
	//    transition. MUST be called before Start.
	state.metaRaft.SetKEKRotationLeader(leader)

	log.Info().
		Str("audit_log", auditPath).
		Str("keystore_dir", keystoreDir).
		Msg("KEK rotation leader wired")
	return nil
}
