package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/raft"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/gritive/GrainFS/internal/transport"
)

func TestMetaCatalogLoadTableReadsMetadataFromWarehouseObject(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket("grainfs-tables"))
	metadata := json.RawMessage(`{"format-version":2,"current-snapshot-id":42}`)
	_, err = backend.PutObject("grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader(metadata), "application/json")
	require.NoError(t, err)

	catalog := NewMetaCatalog(m, backend, "s3://grainfs-tables/warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, catalog.CreateNamespace(ctx, []string{"analytics"}, nil))
	_, err = catalog.CreateTable(ctx, icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         json.RawMessage(`{"wrong":true}`),
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)

	tbl, err := catalog.LoadTable(ctx, icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"})
	require.NoError(t, err)
	require.JSONEq(t, string(metadata), string(tbl.Metadata))
	require.Equal(t, "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json", tbl.MetadataLocation)
}

func TestMetaCatalogLeaderListCommitAndDelete(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })
	require.NoError(t, m.Bootstrap())
	require.NoError(t, m.Start(context.Background()))
	require.Eventually(t, func() bool {
		return m.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	backend, err := storage.NewLocalBackend(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { backend.Close() })
	require.NoError(t, backend.CreateBucket("grainfs-tables"))
	_, err = backend.PutObject("grainfs-tables", "warehouse/analytics/events/metadata/00000.json", bytes.NewReader([]byte(`{"format-version":2,"current-snapshot-id":1}`)), "application/json")
	require.NoError(t, err)
	nextMetadata := []byte(`{"format-version":2,"current-snapshot-id":2}`)
	_, err = backend.PutObject("grainfs-tables", "warehouse/analytics/events/metadata/00001.json", bytes.NewReader(nextMetadata), "application/json")
	require.NoError(t, err)

	catalog := NewMetaCatalog(m, backend, "s3://grainfs-tables/warehouse")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, catalog.CreateNamespace(ctx, []string{"analytics"}, map[string]string{"owner": "eng"}))
	require.NoError(t, catalog.CreateNamespace(ctx, []string{"staging"}, nil))

	namespaces, err := catalog.ListNamespaces(ctx)
	require.NoError(t, err)
	require.ElementsMatch(t, [][]string{{"analytics"}, {"staging"}}, namespaces)
	props, err := catalog.LoadNamespace(ctx, []string{"analytics"})
	require.NoError(t, err)
	require.Equal(t, "eng", props["owner"])

	ident := icebergcatalog.Identifier{Namespace: []string{"analytics"}, Name: "events"}
	_, err = catalog.CreateTable(ctx, ident, icebergcatalog.CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)
	tables, err := catalog.ListTables(ctx, []string{"analytics"})
	require.NoError(t, err)
	require.Equal(t, []icebergcatalog.Identifier{ident}, tables)

	_, err = catalog.CommitTable(ctx, ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		NewMetadataLocation:      "s3://grainfs-tables/warehouse/analytics/events/metadata/00001.json",
	})
	require.NoError(t, err)
	committed, err := catalog.LoadTable(ctx, ident)
	require.NoError(t, err)
	require.Equal(t, "s3://grainfs-tables/warehouse/analytics/events/metadata/00001.json", committed.MetadataLocation)
	require.JSONEq(t, string(nextMetadata), string(committed.Metadata))

	_, err = catalog.CommitTable(ctx, ident, icebergcatalog.CommitTableInput{
		ExpectedMetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		NewMetadataLocation:      "s3://grainfs-tables/warehouse/analytics/events/metadata/00002.json",
	})
	require.ErrorIs(t, err, icebergcatalog.ErrCommitFailed)
	require.ErrorIs(t, catalog.DeleteNamespace(ctx, []string{"analytics"}), icebergcatalog.ErrNamespaceNotEmpty)
	require.NoError(t, catalog.DeleteTable(ctx, ident))
	_, err = catalog.LoadTable(ctx, ident)
	require.ErrorIs(t, err, icebergcatalog.ErrTableNotFound)
	require.NoError(t, catalog.DeleteNamespace(ctx, []string{"analytics"}))
}

func TestMetaCatalogFollowerWriteUsesForwarderTypedResult(t *testing.T) {
	m := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = m.Close() })

	var calls int
	catalog := NewMetaCatalogWithForwarder(m, nil, "s3://grainfs-tables/warehouse", func(context.Context, []byte) error {
		calls++
		return icebergcatalog.ErrNamespaceExists
	})

	err := catalog.CreateNamespace(context.Background(), []string{"analytics"}, nil)
	require.ErrorIs(t, err, icebergcatalog.ErrNamespaceExists)
	require.Equal(t, 1, calls)
}

func TestMetaCatalogFollowerWriteForwarderCommitsOnLeader(t *testing.T) {
	leader := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = leader.Close() })
	require.NoError(t, leader.Bootstrap())
	require.NoError(t, leader.Start(context.Background()))
	require.Eventually(t, func() bool {
		return leader.node.State() == raft.Leader
	}, 2*time.Second, 20*time.Millisecond)

	follower := newSingleMetaRaft(t)
	t.Cleanup(func() { _ = follower.Close() })
	receiver := NewMetaProposeForwardReceiver(leader)
	sender := NewMetaProposeForwardSender(func(_ string, payload []byte) ([]byte, error) {
		return receiver.Handle(&transport.Message{Payload: payload}).Payload, nil
	})
	catalog := NewMetaCatalogWithForwarder(follower, nil, "s3://grainfs-tables/warehouse", func(ctx context.Context, command []byte) error {
		return sender.Send(ctx, []string{"leader"}, command)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	require.NoError(t, catalog.CreateNamespace(ctx, []string{"analytics"}, nil))
	_, ok := leader.FSM().IcebergNamespace([]string{"analytics"})
	require.True(t, ok)
}
