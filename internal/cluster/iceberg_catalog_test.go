package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/nfsexport"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
)

func TestMetaCatalogLoadTableReadsMetadataFromWarehouseObject(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(context.Background(), "grainfs-tables"))
	metadata := json.RawMessage(`{"format-version":2,"current-snapshot-id":42}`)
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader(metadata), "application/json")
	require.NoError(t, err)

	catalog := NewMetaCatalog(m, backend, "s3://grainfs-tables/warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, catalog.CreateNamespace(ctx, "", []string{"analytics"}, nil))
	_, err = catalog.CreateTable(ctx, "", icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         json.RawMessage(`{"wrong":true}`),
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)

	tbl, err := catalog.LoadTable(ctx, "", icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"})
	require.NoError(t, err)
	require.JSONEq(t, string(metadata), string(tbl.Metadata))
	require.Equal(t, "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json", tbl.MetadataLocation)
}

func TestMetaCatalogLoadTableReusesMetadataReadAfterCreate(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { local.Close() })
	backend := &countingGetBackend{Backend: local}
	require.NoError(t, backend.CreateBucket(context.Background(), "grainfs-tables"))
	metadata := json.RawMessage(`{"format-version":2,"current-snapshot-id":42}`)
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader(metadata), "application/json")
	require.NoError(t, err)

	catalog := NewMetaCatalog(m, backend, "s3://grainfs-tables/warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	require.NoError(t, catalog.CreateNamespace(ctx, "", []string{"analytics"}, nil))
	_, err = catalog.CreateTable(ctx, "", ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         json.RawMessage(`{"wrong":true}`),
	})
	require.NoError(t, err)

	tbl, err := catalog.LoadTable(ctx, "", ident)
	require.NoError(t, err)
	require.JSONEq(t, string(metadata), string(tbl.Metadata))
	require.Equal(t, int64(1), backend.gets.Load())
}

func BenchmarkMetaCatalogLoadTableRepeated(b *testing.B) {
	m := newSingleMetaRaft(b)
	b.Cleanup(func() { _ = m.Close() })
	require.NoError(b, m.Bootstrap())
	require.NoError(b, m.Start(context.Background(), nil))
	require.Eventually(b, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	local, err := storage.NewLocalBackend(b.TempDir())
	require.NoError(b, err)
	b.Cleanup(func() { local.Close() })
	backend := &countingGetBackend{Backend: local}
	require.NoError(b, backend.CreateBucket(context.Background(), "grainfs-tables"))
	metadata := bytes.Repeat([]byte(`{"format-version":2,"current-snapshot-id":42}`), 64)
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader(metadata), "application/json")
	require.NoError(b, err)

	catalog := NewMetaCatalog(m, backend, "s3://grainfs-tables/warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	require.NoError(b, catalog.CreateNamespace(ctx, "", []string{"analytics"}, nil))
	_, err = catalog.CreateTable(ctx, "", ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
	})
	require.NoError(b, err)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := catalog.LoadTable(context.Background(), "", ident); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(backend.gets.Load()), "getobject_total")
}

func TestMetaCatalogLeaderListCommitAndDelete(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(context.Background(), "grainfs-tables"))
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader([]byte(`{"format-version":2,"current-snapshot-id":1}`)), "application/json")
	require.NoError(t, err)
	nextMetadata := []byte(`{"format-version":2,"current-snapshot-id":2}`)
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00001.json", bytes.NewReader(nextMetadata), "application/json")
	require.NoError(t, err)

	catalog := NewMetaCatalog(m, backend, "s3://grainfs-tables/warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, catalog.CreateNamespace(ctx, "", []string{"analytics"}, map[string]string{"owner": "eng"}))
	require.NoError(t, catalog.CreateNamespace(ctx, "", []string{"staging"}, nil))

	namespaces, err := catalog.ListNamespaces(ctx, "")
	require.NoError(t, err)
	require.ElementsMatch(t, [][]string{{"analytics"}, {"staging"}}, namespaces)
	props, err := catalog.LoadNamespace(ctx, "", []string{"analytics"})
	require.NoError(t, err)
	require.Equal(t, "eng", props["owner"])

	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	_, err = catalog.CreateTable(ctx, "", ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)
	tables, err := catalog.ListTables(ctx, "", []string{"analytics"})
	require.NoError(t, err)
	require.Equal(t, []icebergcatalog.Identifier{ident}, tables)

	_, err = catalog.CommitTable(ctx, "", ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		NewMetadataLocation:      "s3://grainfs-tables/warehouse/analytics/events/metadata/00001.json",
	})
	require.NoError(t, err)
	committed, err := catalog.LoadTable(ctx, "", ident)
	require.NoError(t, err)
	require.Equal(t, "s3://grainfs-tables/warehouse/analytics/events/metadata/00001.json", committed.MetadataLocation)
	require.JSONEq(t, string(nextMetadata), string(committed.Metadata))

	_, err = catalog.CommitTable(ctx, "", ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		NewMetadataLocation:      "s3://grainfs-tables/warehouse/analytics/events/metadata/00002.json",
	})
	require.ErrorIs(t, err, icebergcatalog.ErrCommitFailed)
	require.ErrorIs(t, catalog.DeleteNamespace(ctx, "", []string{"analytics"}), icebergcatalog.ErrNamespaceNotEmpty)
	require.NoError(t, catalog.DeleteTable(ctx, "", ident))
	_, err = catalog.LoadTable(ctx, "", ident)
	require.ErrorIs(t, err, icebergcatalog.ErrTableNotFound)
	require.NoError(t, catalog.DeleteNamespace(ctx, "", []string{"analytics"}))
}

func TestMetaCatalogFollowerWriteUsesForwarderTypedResult(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	var calls int
	catalog := NewMetaCatalogWithForwarder(m, nil, "s3://grainfs-tables/warehouse", func(context.Context, []byte) error {
		calls++
		return icebergcatalog.ErrNamespaceExists
	})

	err := catalog.CreateNamespace(context.Background(), "", []string{"analytics"}, nil)
	require.ErrorIs(t, err, icebergcatalog.ErrNamespaceExists)
	require.Equal(t, 1, calls)
}

type countingGetBackend struct {
	storage.Backend
	gets atomic.Int64
}

func (b *countingGetBackend) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	b.gets.Add(1)
	return b.Backend.GetObject(ctx, bucket, key)
}

func TestMetaCatalogFollowerWriteForwarderCommitsOnLeader(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })
	receiver := NewMetaProposeForwardReceiver(leader)
	sender := NewMetaProposeForwardSender(func(_ context.Context, _ string, payload []byte) ([]byte, error) {
		return receiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	})
	catalog := NewMetaCatalogWithForwarder(follower, nil, "s3://grainfs-tables/warehouse", func(ctx context.Context, command []byte) error {
		return sender.Send(ctx, []string{"leader"}, command)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, catalog.CreateNamespace(ctx, "", []string{"analytics"}, nil))
	// empty warehouse arg resolves to IcebergDefaultWarehouse ("default").
	_, ok := leader.FSM().IcebergNamespace(IcebergDefaultWarehouse, []string{"analytics"})
	require.True(t, ok)
}

func TestMetaCatalogFollowerCreateTableReturnsForwardedLeaderRead(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket(context.Background(), "grainfs-tables"))
	metadata := json.RawMessage(`{"format-version":2,"current-snapshot-id":7}`)
	metadataLocation := "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json"
	_, err = backend.PutObject(context.Background(), "grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader(metadata), "application/json")
	require.NoError(t, err)

	leaderCatalog := NewMetaCatalog(leader, backend, "s3://grainfs-tables/warehouse")
	leaderReceiver := NewMetaProposeForwardReceiver(leader)
	forwardSender := NewMetaProposeForwardSender(func(_ context.Context, _ string, payload []byte) ([]byte, error) {
		return leaderReceiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	})
	readReceiver := NewMetaCatalogReadReceiver(leaderCatalog)
	readSender := NewMetaCatalogReadSender(func(_ context.Context, _ string, payload []byte) ([]byte, error) {
		return readReceiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	})
	followerCatalog := NewMetaCatalogWithForwarders(
		follower,
		backend,
		"s3://grainfs-tables/warehouse",
		func(ctx context.Context, command []byte) error {
			return forwardSender.Send(ctx, []string{"leader"}, command)
		},
		readSender,
		func() []string { return []string{"leader"} },
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, leaderCatalog.CreateNamespace(ctx, "", []string{"analytics"}, nil))
	tbl, err := followerCatalog.CreateTable(ctx, "", icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}, icebergcatalog.CreateTableInput{
		MetadataLocation: metadataLocation,
	})
	require.NoError(t, err)
	require.Equal(t, metadataLocation, tbl.MetadataLocation)
	require.JSONEq(t, string(metadata), string(tbl.Metadata))
}

func TestMetaCatalogFollowerCreateTableReturnsProvidedMetadataWithoutLeaderObjectRead(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })

	leaderBackend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { leaderBackend.Close() })
	require.NoError(t, leaderBackend.CreateBucket(context.Background(), "grainfs-tables"))

	leaderCatalog := NewMetaCatalog(leader, leaderBackend, "s3://grainfs-tables/warehouse")
	leaderReceiver := NewMetaProposeForwardReceiver(leader)
	forwardSender := NewMetaProposeForwardSender(func(_ context.Context, _ string, payload []byte) ([]byte, error) {
		return leaderReceiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	})
	readReceiver := NewMetaCatalogReadReceiver(leaderCatalog)
	readSender := NewMetaCatalogReadSender(func(_ context.Context, _ string, payload []byte) ([]byte, error) {
		return readReceiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	})
	followerCatalog := NewMetaCatalogWithForwarders(
		follower,
		nil,
		"s3://grainfs-tables/warehouse",
		func(ctx context.Context, command []byte) error {
			return forwardSender.Send(ctx, []string{"leader"}, command)
		},
		readSender,
		func() []string { return []string{"leader"} },
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, leaderCatalog.CreateNamespace(ctx, "", []string{"analytics"}, nil))
	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	metadata := json.RawMessage(`{"format-version":2,"current-snapshot-id":7}`)
	tbl, err := followerCatalog.CreateTable(ctx, "", ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         metadata,
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)
	require.Equal(t, ident, tbl.Identifier)
	require.JSONEq(t, string(metadata), string(tbl.Metadata))
	require.Equal(t, "2", tbl.Properties["format-version"])
}

func TestMetaForwarderSkipsNonLeaderAndCommitsBucketAssignment(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	payload, err := encodeMetaPutBucketAssignmentCmd("photos", "group-0")
	require.NoError(t, err)
	command, err := encodeMetaCmd(MetaCmdTypePutBucketAssignment, payload)
	require.NoError(t, err)

	leaderReceiver := NewMetaProposeForwardReceiver(leader)
	sender := NewMetaProposeForwardSender(func(_ context.Context, peer string, payload []byte) ([]byte, error) {
		if peer == "follower" {
			return encodeMetaForwardReply(raft.ErrNotLeader), nil
		}
		return leaderReceiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, sender.Send(ctx, []string{"follower", "leader"}, command))
	require.Equal(t, "group-0", leader.FSM().BucketAssignments()["photos"])
}

func TestMetaForwardReceiverRejectsRawGatedCommand(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	payload, err := nfsexport.EncodeUpsertPayload("photos", nfsexport.Config{})
	require.NoError(t, err)
	command, err := encodeMetaCmd(MetaCmdTypeNfsExportCreate, payload)
	require.NoError(t, err)

	reply := NewMetaProposeForwardReceiver(leader).Handle(&transport.Message{Payload: command})
	_, err = decodeMetaForwardReplyWithIndex(reply.Payload)
	require.ErrorIs(t, err, compat.ErrCapabilityRejected)
}

func TestMetaForwardReceiverAllowsLegacyRawMigrationCutover(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	payload := encodeMigrationCutoverPayload("photos", time.Unix(100, 0).UnixNano())
	command, err := encodeMetaCmd(MetaCmdTypeMigrationCutover, payload)
	require.NoError(t, err)

	reply := NewMetaProposeForwardReceiver(leader).Handle(&transport.Message{Payload: command})
	_, err = decodeMetaForwardReplyWithIndex(reply.Payload)
	require.Error(t, err)
	require.NotErrorIs(t, err, compat.ErrCapabilityRejected)
}

func TestMetaForwardReceiverRevalidatesGateOnLeader(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	cfg := raft.Configuration{Servers: []raft.Server{
		{ID: "node-a", Suffrage: raft.Voter},
		{ID: "node-b", Suffrage: raft.Voter},
	}}
	gate.SetMetaRaftSnapshot(2, cfg)
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("node-a"),
		Capabilities: map[string]bool{
			compat.CapabilityNfsExportCreateV1: true,
		},
		LastSeen: time.Now(),
		Ready:    true,
	})
	leader.SetCapabilityGate(gate)

	payload, err := nfsexport.EncodeUpsertPayload("photos", nfsexport.Config{})
	require.NoError(t, err)
	command, err := encodeMetaCmd(MetaCmdTypeNfsExportCreate, payload)
	require.NoError(t, err)
	plan := compat.GatePlan{
		Capability: compat.CapabilityNfsExportCreateV1,
		Scope:      compat.ScopeMetaRaft,
		Severity:   compat.SeverityHard,
		Operation:  compat.OperationNfsExportCreate,
		ConfigID:   raftConfigurationID(cfg),
	}

	reply := NewMetaProposeForwardReceiver(leader).Handle(&transport.Message{
		Payload: encodeMetaForwardRequest(command, &plan),
	})
	_, err = decodeMetaForwardReplyWithIndex(reply.Payload)
	require.ErrorIs(t, err, compat.ErrCapabilityRejected)
}

func TestMetaForwardReceiverRefreshesGateBeforeGatedProposal(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	gate := NewCapabilityGate(compat.DefaultRegistry, time.Minute)
	cfg := raft.Configuration{Servers: []raft.Server{{ID: "node-a", Suffrage: raft.Voter}}}
	gate.SetMetaRaftSnapshot(1, cfg)
	gate.ReportEvidence(compat.Evidence{
		NodeID: compat.NodeID("node-a"),
		Capabilities: map[string]bool{
			compat.CapabilityNfsExportCreateV1: true,
		},
		LastSeen: time.Now(),
		Ready:    true,
	})
	leader.SetCapabilityGate(gate)

	command, err := encodeMetaCmd(MetaCmdTypeNoOp, nil)
	require.NoError(t, err)
	plan := compat.GatePlan{
		Capability: compat.CapabilityNfsExportCreateV1,
		Scope:      compat.ScopeMetaRaft,
		Severity:   compat.SeverityHard,
		Operation:  compat.OperationNfsExportCreate,
		ConfigID:   raftConfigurationID(cfg),
	}

	reply := NewMetaProposeForwardReceiver(leader).
		WithGateRefresh(func() {
			gate.SetMetaRaftSnapshot(2, cfg)
		}).
		Handle(&transport.Message{Payload: encodeMetaForwardRequest(command, &plan)})
	_, err = decodeMetaForwardReplyWithIndex(reply.Payload)
	require.NoError(t, err)
}

func TestMetaForwardReplyPreservesNonIcebergApplyError(t *testing.T) {
	_, err := decodeMetaForwardReplyWithIndex(encodeMetaForwardReplyWithIndex(7, fmt.Errorf("meta_fsm: apply failed")))
	require.Error(t, err)
	var applyErr MetaForwardApplyError
	require.ErrorAs(t, err, &applyErr)
	require.Equal(t, "meta_fsm: apply failed", applyErr.Error())
	require.False(t, errors.Is(err, icebergcatalog.ErrServiceUnavailable))
}

func TestMetaForwardReplyPreservesContextErrors(t *testing.T) {
	_, err := decodeMetaForwardReplyWithIndex(encodeMetaForwardReplyWithIndex(7, context.DeadlineExceeded))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	var applyErr MetaForwardApplyError
	require.False(t, errors.As(err, &applyErr))

	_, err = decodeMetaForwardReplyWithIndex(encodeMetaForwardReplyWithIndex(7, context.Canceled))
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, errors.As(err, &applyErr))
}

func TestForwardingBucketAssignerForwardsFromFollower(t *testing.T) {
	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })

	var forwarded []byte
	assigner := NewForwardingBucketAssigner(follower, func(_ context.Context, command []byte) error {
		forwarded = append([]byte(nil), command...)
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = follower.FSM().applyCmd(command)
		}()
		return nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, assigner.ProposeBucketAssignment(ctx, "photos", "group-0"))

	cmd := clusterpb.GetRootAsMetaCmd(forwarded, 0)
	require.Equal(t, MetaCmdTypePutBucketAssignment, cmd.Type())
	assignment := clusterpb.GetRootAsMetaPutBucketAssignmentCmd(cmd.DataBytes(), 0).Entry(nil)
	require.NotNil(t, assignment)
	require.Equal(t, "photos", string(assignment.Bucket()))
	require.Equal(t, "group-0", string(assignment.GroupId()))
}

func TestForwardingBucketAssignerTimesOutWaitingForLocalApply(t *testing.T) {
	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })

	prev := bucketAssignmentLocalApplyTimeout
	bucketAssignmentLocalApplyTimeout = 20 * time.Millisecond
	t.Cleanup(func() { bucketAssignmentLocalApplyTimeout = prev })

	assigner := NewForwardingBucketAssigner(follower, func(_ context.Context, _ []byte) error {
		return nil
	})

	start := time.Now()
	err := assigner.ProposeBucketAssignment(context.Background(), "photos", "group-0")
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), time.Second)
}

func TestForwardingObjectIndexProposerForwardsFromFollower(t *testing.T) {
	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })
	tracePath := filepath.Join(t.TempDir(), "put-trace.jsonl")
	t.Setenv("GRAINFS_PUT_TRACE_FILE", tracePath)
	reloadPutTraceSinkForTest()
	t.Cleanup(reloadPutTraceSinkForTest)

	entry := ObjectIndexEntry{
		Bucket:           "photos",
		Key:              "img.jpg",
		VersionID:        "v1",
		PlacementGroupID: "group-1",
		Size:             42,
		ETag:             "etag",
	}

	var forwarded []byte
	proposer := NewForwardingObjectIndexProposer(follower, func(_ context.Context, command []byte) error {
		forwarded = append([]byte(nil), command...)
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = follower.FSM().applyCmd(command)
		}()
		return nil
	})

	ctx := ContextWithPutTrace(context.Background(), PutTraceRequest{
		Bucket:      "photos",
		Key:         "img.jpg",
		GroupID:     "group-1",
		Ingress:     PutTraceIngressReceiver,
		SizeClass:   PutTraceSizeSmall,
		ForwardMode: PutTraceForwardFrame,
	})
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	require.NoError(t, proposer.ProposeObjectIndex(ctx, entry, false))

	cmd := clusterpb.GetRootAsMetaCmd(forwarded, 0)
	require.Equal(t, MetaCmdTypePutObjectIndex, cmd.Type())
	got, ok := follower.FSM().ObjectIndexVersion("photos", "img.jpg", "v1")
	require.True(t, ok)
	require.Equal(t, "group-1", got.PlacementGroupID)
	events := readPutTraceEvents(t, tracePath)
	requirePutTraceStage(t, events, PutTraceStageMetaIndexEncode)
	requirePutTraceStage(t, events, PutTraceStageMetaIndexForward)
	requirePutTraceStage(t, events, PutTraceStageMetaIndexWaitLocal)
}

func TestForwardingObjectIndexProposerUsesForwardedApplyIndex(t *testing.T) {
	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })

	entry := ObjectIndexEntry{
		Bucket:           "photos",
		Key:              "img.jpg",
		VersionID:        "v1",
		PlacementGroupID: "group-1",
		Size:             42,
		ETag:             "etag",
	}

	var forwarded []byte
	proposer := NewForwardingObjectIndexProposer(follower, func(context.Context, []byte) error {
		t.Fatal("legacy forwarder should not be used when an index forwarder is configured")
		return nil
	}).WithIndexForwarder(func(_ context.Context, command []byte) (uint64, error) {
		forwarded = append([]byte(nil), command...)
		go func() {
			time.Sleep(20 * time.Millisecond)
			_ = follower.FSM().applyCmd(command)
			follower.lastApplied.Store(42)
			follower.applyNotifyMu.Lock()
			old := follower.applyNotify
			follower.applyNotify = make(chan struct{})
			follower.applyNotifyMu.Unlock()
			close(old)
		}()
		return 42, nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, proposer.ProposeObjectIndex(ctx, entry, false))

	cmd := clusterpb.GetRootAsMetaCmd(forwarded, 0)
	require.Equal(t, MetaCmdTypePutObjectIndex, cmd.Type())
	got, ok := follower.FSM().ObjectIndexVersion("photos", "img.jpg", "v1")
	require.True(t, ok)
	require.Equal(t, "group-1", got.PlacementGroupID)
}

func BenchmarkForwardingObjectIndexProposerApplyWait(b *testing.B) {
	bench := func(b *testing.B, useForwardedIndex bool) {
		tr := newMetaTransportFake()
		follower, err := NewMetaRaft(MetaRaftConfig{
			NodeID:    "node-0",
			Peers:     nil,
			DataDir:   b.TempDir(),
			Transport: tr,
		})
		require.NoError(b, err)
		tr.register("node-0", follower)
		b.Cleanup(func() { _ = follower.Close() })

		proposer := NewForwardingObjectIndexProposer(follower, func(_ context.Context, command []byte) error {
			go func() {
				time.Sleep(100 * time.Microsecond)
				_ = follower.FSM().applyCmd(command)
			}()
			return nil
		})
		if useForwardedIndex {
			var idx uint64
			proposer.WithIndexForwarder(func(_ context.Context, command []byte) (uint64, error) {
				idx++
				appliedIndex := idx
				go func() {
					time.Sleep(100 * time.Microsecond)
					_ = follower.FSM().applyCmd(command)
					follower.lastApplied.Store(appliedIndex)
					follower.applyNotifyMu.Lock()
					old := follower.applyNotify
					follower.applyNotify = make(chan struct{})
					follower.applyNotifyMu.Unlock()
					close(old)
				}()
				return appliedIndex, nil
			})
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			entry := ObjectIndexEntry{
				Bucket:           "photos",
				Key:              fmt.Sprintf("img-%d.jpg", i),
				VersionID:        "v1",
				PlacementGroupID: "group-1",
				Size:             42,
				ETag:             "etag",
			}
			if err := proposer.ProposeObjectIndex(context.Background(), entry, false); err != nil {
				b.Fatal(err)
			}
		}
	}

	b.Run("poll_fsm", func(b *testing.B) { bench(b, false) })
	b.Run("forwarded_apply_index", func(b *testing.B) { bench(b, true) })
}

// TestMetaCatalog_TwoWarehousesIsolated verifies that namespaces and tables
// created in one warehouse are not visible in another.
func TestMetaCatalog_TwoWarehousesIsolated(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	catA := NewMetaCatalog(m, backend, "s3://bucket/warehouse-a")
	catB := NewMetaCatalog(m, backend, "s3://bucket/warehouse-b")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Use explicit warehouse names to verify cross-warehouse isolation.
	// (Passing "" resolves to IcebergDefaultWarehouse for both catalogs,
	// so explicit keys are required here.)
	whA := "warehouse-a"
	whB := "warehouse-b"

	// Create namespace "analytics" in warehouse-a only.
	require.NoError(t, catA.CreateNamespace(ctx, whA, []string{"analytics"}, map[string]string{"owner": "team-a"}))

	// warehouse-a sees it.
	props, err := catA.LoadNamespace(ctx, whA, []string{"analytics"})
	require.NoError(t, err)
	require.Equal(t, "team-a", props["owner"])

	// warehouse-b does NOT see it.
	_, err = catB.LoadNamespace(ctx, whB, []string{"analytics"})
	require.ErrorIs(t, err, icebergcatalog.ErrNamespaceNotFound, "warehouse-b should not see warehouse-a namespace")

	// ListNamespaces is also isolated.
	nsA, err := catA.ListNamespaces(ctx, whA)
	require.NoError(t, err)
	require.Len(t, nsA, 1)
	nsB, err := catB.ListNamespaces(ctx, whB)
	require.NoError(t, err)
	require.Empty(t, nsB)

	// Create same namespace in warehouse-b independently.
	require.NoError(t, catB.CreateNamespace(ctx, whB, []string{"analytics"}, map[string]string{"owner": "team-b"}))
	propsB, err := catB.LoadNamespace(ctx, whB, []string{"analytics"})
	require.NoError(t, err)
	require.Equal(t, "team-b", propsB["owner"])

	// Delete in warehouse-a does not affect warehouse-b.
	require.NoError(t, catA.DeleteNamespace(ctx, whA, []string{"analytics"}))
	_, err = catA.LoadNamespace(ctx, whA, []string{"analytics"})
	require.ErrorIs(t, err, icebergcatalog.ErrNamespaceNotFound)
	propsB2, err := catB.LoadNamespace(ctx, whB, []string{"analytics"})
	require.NoError(t, err)
	require.Equal(t, "team-b", propsB2["owner"])
}

// TestMetaCatalog_EmptyWarehouseResolvesToDefault verifies that passing "" as
// the warehouse argument to MetaCatalog methods resolves to
// IcebergDefaultWarehouse ("default"), not to the constructor's S3 URI.
func TestMetaCatalog_EmptyWarehouseResolvesToDefault(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })

	// Constructor warehouse is an S3 URI — different from IcebergDefaultWarehouse.
	catalog := NewMetaCatalog(m, backend, "s3://example-warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// CreateNamespace with "" warehouse — should land under "default", not under the S3 URI.
	require.NoError(t, catalog.CreateNamespace(ctx, "", []string{"ns"}, nil))

	// Verify via FSM using IcebergDefaultWarehouse key.
	_, ok := m.FSM().IcebergNamespace(IcebergDefaultWarehouse, []string{"ns"})
	require.True(t, ok, "namespace should be stored under IcebergDefaultWarehouse")

	// Verify it is NOT stored under the constructor S3 URI key.
	_, notOK := m.FSM().IcebergNamespace("s3://example-warehouse", []string{"ns"})
	require.False(t, notOK, "namespace should NOT be stored under the S3-URI warehouse key")
}

// TestMetaCatalog_DefaultWarehouseIsConst is an F15 regression test.
// NewMetaCatalog (and its variants) must always return IcebergDefaultWarehouse
// from Warehouse(), regardless of the s3URLPrefix passed at construction.
// The S3 URI prefix must be accessible via S3URLPrefix() unchanged.
func TestMetaCatalog_DefaultWarehouseIsConst(t *testing.T) {
	const s3Prefix = "s3://grainfs-tables/warehouse"

	cat := NewMetaCatalog(nil, nil, s3Prefix)
	assert.Equal(t, IcebergDefaultWarehouse, cat.Warehouse(),
		"Warehouse() must always return IcebergDefaultWarehouse (F15)")
	assert.Equal(t, s3Prefix, cat.S3URLPrefix(),
		"S3URLPrefix() must return the constructor s3URLPrefix arg unchanged (F15)")

	catFwd := NewMetaCatalogWithForwarder(nil, nil, s3Prefix, nil)
	assert.Equal(t, IcebergDefaultWarehouse, catFwd.Warehouse(),
		"NewMetaCatalogWithForwarder: Warehouse() must return IcebergDefaultWarehouse (F15)")
	assert.Equal(t, s3Prefix, catFwd.S3URLPrefix(),
		"NewMetaCatalogWithForwarder: S3URLPrefix() must return s3URLPrefix (F15)")

	catFwds := NewMetaCatalogWithForwarders(nil, nil, s3Prefix, nil, nil, nil)
	assert.Equal(t, IcebergDefaultWarehouse, catFwds.Warehouse(),
		"NewMetaCatalogWithForwarders: Warehouse() must return IcebergDefaultWarehouse (F15)")
	assert.Equal(t, s3Prefix, catFwds.S3URLPrefix(),
		"NewMetaCatalogWithForwarders: S3URLPrefix() must return s3URLPrefix (F15)")
}

// TestMetaCatalog_MetadataCache_WarehouseScoped verifies that the metadata
// cache uses warehouse-scoped keys (F18). Two warehouses with the same
// (namespace, table) identifier but different metadata must not cross-contaminate.
func TestMetaCatalog_MetadataCache_WarehouseScoped(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background(), nil))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	local, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { local.Close() })
	require.NoError(t, local.CreateBucket(context.Background(), "bucket"))

	metaA := json.RawMessage(`{"format-version":2,"table-uuid":"aaa"}`)
	metaB := json.RawMessage(`{"format-version":2,"table-uuid":"bbb"}`)
	locA := "s3://bucket/warehouse-a/ns/events/metadata/00000.json"
	locB := "s3://bucket/warehouse-b/ns/events/metadata/00000.json"

	// Write both metadata objects to the backend.
	_, err = local.PutObject(context.Background(), "bucket", "warehouse-a/ns/events/metadata/00000.json", bytes.NewReader(metaA), "application/json")
	require.NoError(t, err)
	_, err = local.PutObject(context.Background(), "bucket", "warehouse-b/ns/events/metadata/00000.json", bytes.NewReader(metaB), "application/json")
	require.NoError(t, err)

	catA := NewMetaCatalog(m, local, "s3://bucket/warehouse-a")
	catB := NewMetaCatalog(m, local, "s3://bucket/warehouse-b")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Create namespaces and tables under explicit warehouse keys.
	whA, whB := "warehouse-a", "warehouse-b"
	require.NoError(t, catA.CreateNamespace(ctx, whA, []string{"ns"}, nil))
	require.NoError(t, catB.CreateNamespace(ctx, whB, []string{"ns"}, nil))

	_, err = catA.CreateTable(ctx, whA, icebergcatalog.Identifier{Namespace: []string{"ns"}, Name: "events"},
		icebergcatalog.CreateTableInput{MetadataLocation: locA, Metadata: metaA})
	require.NoError(t, err)
	_, err = catB.CreateTable(ctx, whB, icebergcatalog.Identifier{Namespace: []string{"ns"}, Name: "events"},
		icebergcatalog.CreateTableInput{MetadataLocation: locB, Metadata: metaB})
	require.NoError(t, err)

	// Load from warehouse-a: metadata must be metaA, not metaB.
	tblA, err := catA.LoadTable(ctx, whA, icebergcatalog.Identifier{Namespace: []string{"ns"}, Name: "events"})
	require.NoError(t, err)
	assert.JSONEq(t, string(metaA), string(tblA.Metadata), "warehouse-a LoadTable must return warehouse-a metadata")

	// Load from warehouse-b: metadata must be metaB, not metaA.
	tblB, err := catB.LoadTable(ctx, whB, icebergcatalog.Identifier{Namespace: []string{"ns"}, Name: "events"})
	require.NoError(t, err)
	assert.JSONEq(t, string(metaB), string(tblB.Metadata), "warehouse-b LoadTable must return warehouse-b metadata")
}
