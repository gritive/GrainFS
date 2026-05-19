package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/migration"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/raft"
)

func TestCapabilityGateRequiresAllConfigMembers(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 10*time.Second)
	now := time.Unix(100, 0)
	cfg := raft.Configuration{Servers: []raft.Server{
		{ID: "node-1", Suffrage: raft.Voter},
		{ID: "node-2", Suffrage: raft.NonVoter},
	}}
	g.SetMetaRaftSnapshot(7, cfg)
	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-1",
		Capabilities: map[string]bool{compat.CapabilityMigrationCutoverV1: true},
		LastSeen:     now,
		Ready:        true,
	})

	plan, err := g.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, now)
	require.Error(t, err)
	require.Equal(t, raftConfigurationID(cfg), plan.ConfigID)
	require.Equal(t, []compat.NodeID{"node-2"}, plan.Unknown)
}

func TestCapabilityGateRejectsStaleEvidence(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
	cfg := raft.Configuration{Servers: []raft.Server{{ID: "node-1", Suffrage: raft.Voter}}}
	g.SetMetaRaftSnapshot(9, cfg)
	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-1",
		Capabilities: map[string]bool{compat.CapabilityMigrationCutoverV1: true},
		LastSeen:     time.Unix(10, 0),
		Ready:        true,
	})

	plan, err := g.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, time.Unix(20, 0))
	require.Error(t, err)
	require.Equal(t, 1, len(plan.Stale))
	require.Equal(t, compat.NodeID("node-1"), plan.Stale[0].NodeID)
}

func TestCapabilityGateSetTTLExtendsStaleWindow(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
	cfg := raft.Configuration{Servers: []raft.Server{{ID: "node-1", Suffrage: raft.Voter}}}
	seenAt := time.Unix(10, 0)
	g.SetMetaRaftSnapshot(9, cfg)
	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-1",
		Capabilities: map[string]bool{compat.CapabilityMigrationCutoverV1: true},
		LastSeen:     seenAt,
		Ready:        true,
	})

	_, err := g.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, seenAt.Add(20*time.Second))
	require.Error(t, err)

	g.SetTTL(30 * time.Second)
	plan, err := g.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, seenAt.Add(20*time.Second))
	require.NoError(t, err)
	require.True(t, plan.Allowed())
}

func TestCapabilityGateAllowsReadyFreshMembers(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
	now := time.Unix(10, 0)
	cfg := raft.Configuration{Servers: []raft.Server{{ID: "node-1", Suffrage: raft.Voter}}}
	g.SetMetaRaftSnapshot(11, cfg)
	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-1",
		Capabilities: map[string]bool{compat.CapabilityMigrationCutoverV1: true},
		LastSeen:     now,
		Ready:        true,
	})

	plan, err := g.RequireMetaRaftCapability(compat.CapabilityMigrationCutoverV1, compat.OperationMigrationCutover, now)
	require.NoError(t, err)
	require.True(t, plan.Allowed())
	require.Equal(t, raftConfigurationID(cfg), plan.ConfigID)
}

func TestCapabilityGateEvidenceSnapshot(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
	now := time.Unix(10, 0)
	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-1",
		Capabilities: map[string]bool{compat.CapabilityMultipartListingV1: true},
		LastSeen:     now,
		Ready:        true,
	})
	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-2",
		Capabilities: map[string]bool{compat.CapabilityMultipartListingV1: true},
		LastSeen:     now,
		Ready:        false,
	})

	snap := g.EvidenceSnapshot()
	require.True(t, snap["node-1"][compat.CapabilityMultipartListingV1])
	require.False(t, snap["node-2"][compat.CapabilityMultipartListingV1], "Ready=false must mask ready capabilities")

	snap["node-1"][compat.CapabilityMultipartListingV1] = false
	again := g.EvidenceSnapshot()
	require.True(t, again["node-1"][compat.CapabilityMultipartListingV1], "snapshot must not alias gate state")
}

func TestCapabilityGateRequiresPeerTransportCapability(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
	now := time.Unix(10, 0)
	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-1",
		Capabilities: map[string]bool{compat.CapabilityMultipartListingV1: true},
		LastSeen:     now,
		Ready:        true,
	})

	plan, err := g.RequirePeerTransportCapability(compat.CapabilityMultipartListingV1, compat.OperationListParts, []string{"node-1", "node-2"}, now)
	require.Error(t, err)
	require.Equal(t, compat.ScopePeerTransport, plan.Scope)
	require.Equal(t, []compat.NodeID{"node-2"}, plan.Unknown)

	g.ReportEvidence(compat.Evidence{
		NodeID:       "node-2",
		Capabilities: map[string]bool{compat.CapabilityMultipartListingV1: true},
		LastSeen:     now,
		Ready:        true,
	})
	plan, err = g.RequirePeerTransportCapability(compat.CapabilityMultipartListingV1, compat.OperationListParts, []string{"node-1", "node-2"}, now)
	require.NoError(t, err)
	require.True(t, plan.Allowed())
}

func BenchmarkCapabilityGate_RequirePeerTransportCapability(b *testing.B) {
	cases := []int{3, 6, 12}
	now := time.Unix(10, 0)
	for _, peersCount := range cases {
		b.Run(fmt.Sprintf("peers_%d", peersCount), func(b *testing.B) {
			g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
			peers := make([]string, peersCount)
			for i := range peers {
				peer := fmt.Sprintf("node-%02d", i)
				peers[i] = peer
				g.ReportEvidence(compat.Evidence{
					NodeID:       compat.NodeID(peer),
					Capabilities: map[string]bool{compat.CapabilityMultipartListingV1: true},
					LastSeen:     now,
					Ready:        true,
				})
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				plan, err := g.RequirePeerTransportCapability(compat.CapabilityMultipartListingV1, compat.OperationListParts, peers, now)
				if err != nil || !plan.Allowed() {
					b.Fatalf("gate rejected: plan=%+v err=%v", plan, err)
				}
			}
		})
	}
}

func TestCapabilityGateRejectsAfterConfigEpochChange(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
	cfg := raft.Configuration{Servers: []raft.Server{{ID: "node-1", Suffrage: raft.Voter}}}
	g.SetMetaRaftSnapshot(11, cfg)
	plan := compat.GatePlan{ConfigID: raftConfigurationID(cfg)}
	g.SetMetaRaftSnapshot(12, raft.Configuration{Servers: []raft.Server{
		{ID: "node-1", Suffrage: raft.Voter},
		{ID: "node-2", Suffrage: raft.NonVoter},
	}})
	require.Error(t, g.ValidatePlanStillCurrent(plan))
}

func TestCapabilityGateAllowsSameConfigAtDifferentCommittedIndex(t *testing.T) {
	g := NewCapabilityGate(compat.DefaultRegistry, 5*time.Second)
	cfg := raft.Configuration{Servers: []raft.Server{{ID: "node-1", Suffrage: raft.Voter}}}
	g.SetMetaRaftSnapshot(11, cfg)
	plan := compat.GatePlan{ConfigID: raftConfigurationID(cfg)}
	g.SetMetaRaftSnapshot(12, cfg)
	require.NoError(t, g.ValidatePlanStillCurrent(plan))
}

func TestMetaFSMCapabilityEvidenceRequiresMigrationAndIAMWiring(t *testing.T) {
	f := NewMetaFSM()
	ev := f.CapabilityEvidence("node-1", time.Unix(10, 0))
	if ev.Capabilities[compat.CapabilityMigrationCutoverV1] {
		t.Fatal("migration_cutover_v1 must not be advertised before IAM and migration are wired")
	}
}

func TestMetaFSMCapabilityEvidenceAdvertisesAfterWiring(t *testing.T) {
	f := NewMetaFSM()
	iamStore := iam.NewStore()
	f.SetIAM(iamStore, iam.NewApplier(iamStore, nil))
	f.SetMigration(migration.NewJobStore(newMigrationTestDB(t)))
	ev := f.CapabilityEvidence("node-1", time.Unix(10, 0))
	require.True(t, ev.Capabilities[compat.CapabilityMigrationCutoverV1])
	require.True(t, ev.Ready)
}

func TestMetaFSMCapabilityEvidenceAdvertisesNfsExportCreateAfterStoreWiring(t *testing.T) {
	f := NewMetaFSM()
	ev := f.CapabilityEvidence("node-1", time.Unix(10, 0))
	require.False(t, ev.Capabilities[compat.CapabilityNfsExportCreateV1])

	db, err := badger.Open(badger.DefaultOptions(t.TempDir()).WithLogger(nil))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	store, err := nfsexport.OpenStore(db)
	require.NoError(t, err)
	f.SetExportStore(store)

	ev = f.CapabilityEvidence("node-1", time.Unix(10, 0))
	require.True(t, ev.Capabilities[compat.CapabilityNfsExportCreateV1])
}

func TestMetaFSMCapabilityEvidenceAdvertisesMultipartListing(t *testing.T) {
	f := NewMetaFSM()
	ev := f.CapabilityEvidence("node-1", time.Unix(10, 0))
	require.True(t, ev.Capabilities[compat.CapabilityMultipartListingV1])
	require.True(t, ev.Ready)
}
