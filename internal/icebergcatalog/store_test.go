package icebergcatalog

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func openTestStore(t *testing.T) (*Store, func() *Store) {
	t.Helper()
	dir := t.TempDir()
	db, err := badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
	require.NoError(t, err)
	reopen := func() *Store {
		require.NoError(t, db.Close())
		db, err = badger.Open(badger.DefaultOptions(dir).WithLogger(nil))
		require.NoError(t, err)
		return NewStore(db, "s3://grainfs-tables/warehouse")
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewStore(db, "s3://grainfs-tables/warehouse"), reopen
}

func TestStore_NamespaceLifecyclePersistsAcrossRestart(t *testing.T) {
	ctx := context.Background()
	store, reopen := openTestStore(t)

	require.NoError(t, store.CreateNamespace(ctx, "", []string{"default"}, map[string]string{"owner": "duckdb"}))
	store = reopen()

	props, err := store.LoadNamespace(ctx, "", []string{"default"})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"owner": "duckdb"}, props)

	namespaces, err := store.ListNamespaces(ctx, "")
	require.NoError(t, err)
	require.Equal(t, [][]string{{"default"}}, namespaces)
}

func TestStore_TableCreateLoadAndCommitCAS(t *testing.T) {
	ctx := context.Background()
	store, reopen := openTestStore(t)
	require.NoError(t, store.CreateNamespace(ctx, "", []string{"default"}, nil))

	ident := Identifier{Namespace: []string{"default"}, Name: "t"}
	initial := json.RawMessage(`{"format-version":2,"location":"s3://grainfs-tables/warehouse/default/t"}`)
	created, err := store.CreateTable(ctx, "", ident, CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/default/t/metadata/00000.json",
		Metadata:         initial,
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)
	require.Equal(t, ident, created.Identifier)
	require.Equal(t, "s3://grainfs-tables/warehouse/default/t/metadata/00000.json", created.MetadataLocation)

	next := json.RawMessage(`{"format-version":2,"current-snapshot-id":1}`)
	committed, err := store.CommitTable(ctx, "", ident, CommitTableInput{
		ExpectedMetadataLocation: "s3://grainfs-tables/warehouse/default/t/metadata/00000.json",
		NewMetadataLocation:      "s3://grainfs-tables/warehouse/default/t/metadata/00001.json",
		Metadata:                 next,
	})
	require.NoError(t, err)
	require.Equal(t, "s3://grainfs-tables/warehouse/default/t/metadata/00001.json", committed.MetadataLocation)
	store = reopen()

	_, err = store.CommitTable(ctx, "", ident, CommitTableInput{
		ExpectedMetadataLocation: "s3://grainfs-tables/warehouse/default/t/metadata/00000.json",
		NewMetadataLocation:      "s3://grainfs-tables/warehouse/default/t/metadata/00002.json",
		Metadata:                 json.RawMessage(`{"format-version":2,"current-snapshot-id":2}`),
	})
	require.ErrorIs(t, err, ErrCommitFailed)

	loaded, err := store.LoadTable(ctx, "", ident)
	require.NoError(t, err)
	require.Equal(t, "s3://grainfs-tables/warehouse/default/t/metadata/00001.json", loaded.MetadataLocation)
}

func TestStore_ExportLegacyRowsPreservesNamespaceAndTableData(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)
	require.NoError(t, store.CreateNamespace(ctx, "", []string{"analytics"}, map[string]string{"owner": "eng"}))
	ident := Identifier{Namespace: []string{"analytics"}, Name: "events"}
	metadata := json.RawMessage(`{"format-version":2,"location":"s3://grainfs-tables/warehouse/analytics/events"}`)
	_, err := store.CreateTable(ctx, "", ident, CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json",
		Metadata:         metadata,
		Properties:       map[string]string{"format-version": "2"},
	})
	require.NoError(t, err)

	exported, err := store.ExportLegacyRows(ctx)
	require.NoError(t, err)
	require.Len(t, exported.Namespaces, 1)
	require.Equal(t, []string{"analytics"}, exported.Namespaces[0].Namespace)
	require.Equal(t, map[string]string{"owner": "eng"}, exported.Namespaces[0].Properties)
	require.Len(t, exported.Tables, 1)
	require.Equal(t, ident, exported.Tables[0].Identifier)
	require.Equal(t, "s3://grainfs-tables/warehouse/analytics/events/metadata/00000.json", exported.Tables[0].MetadataLocation)
	require.JSONEq(t, string(metadata), string(exported.Tables[0].Metadata))
	require.Equal(t, map[string]string{"format-version": "2"}, exported.Tables[0].Properties)
}

func TestStore_DeleteTableAndNamespace(t *testing.T) {
	ctx := context.Background()
	store, reopen := openTestStore(t)
	require.NoError(t, store.CreateNamespace(ctx, "", []string{"default"}, nil))

	ident := Identifier{Namespace: []string{"default"}, Name: "t"}
	_, err := store.CreateTable(ctx, "", ident, CreateTableInput{
		MetadataLocation: "s3://grainfs-tables/warehouse/default/t/metadata/00000.json",
		Metadata:         json.RawMessage(`{"format-version":2}`),
	})
	require.NoError(t, err)

	require.ErrorIs(t, store.DeleteNamespace(ctx, "", []string{"default"}), ErrNamespaceNotEmpty)
	require.NoError(t, store.DeleteTable(ctx, "", ident))
	store = reopen()

	_, err = store.LoadTable(ctx, "", ident)
	require.ErrorIs(t, err, ErrTableNotFound)
	require.NoError(t, store.DeleteNamespace(ctx, "", []string{"default"}))
	_, err = store.LoadNamespace(ctx, "", []string{"default"})
	require.ErrorIs(t, err, ErrNamespaceNotFound)
}

func TestStore_ErrorsAreTyped(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)

	_, err := store.LoadNamespace(ctx, "", []string{"missing"})
	require.ErrorIs(t, err, ErrNamespaceNotFound)

	_, err = store.LoadTable(ctx, "", Identifier{Namespace: []string{"missing"}, Name: "t"})
	require.True(t, errors.Is(err, ErrNamespaceNotFound) || errors.Is(err, ErrTableNotFound))
}

// TestLegacyStore_RejectsNonDefaultWarehouse verifies that F22 is fixed:
// Store is a single-warehouse implementation and must reject calls for any
// warehouse other than "default" or "". Accepting silently would allow writes
// intended for warehouse-A to be read back as warehouse-B data.
func TestLegacyStore_RejectsNonDefaultWarehouse(t *testing.T) {
	ctx := context.Background()
	store, _ := openTestStore(t)

	// Non-default warehouse must always error.
	err := store.CreateNamespace(ctx, "warehouse-a", []string{"ns"}, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	_, err = store.LoadNamespace(ctx, "warehouse-a", []string{"ns"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	_, err = store.ListNamespaces(ctx, "warehouse-a")
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	err = store.DeleteNamespace(ctx, "warehouse-a", []string{"ns"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	ident := Identifier{Namespace: []string{"ns"}, Name: "t"}
	_, err = store.CreateTable(ctx, "warehouse-a", ident, CreateTableInput{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	_, err = store.LoadTable(ctx, "warehouse-a", ident)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	_, err = store.ListTables(ctx, "warehouse-a", []string{"ns"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	err = store.DeleteTable(ctx, "warehouse-a", ident)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	_, err = store.CommitTable(ctx, "warehouse-a", ident, CommitTableInput{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot handle warehouse")

	// "" and "default" must be accepted (no error from warehouse check).
	require.NoError(t, store.CreateNamespace(ctx, "", []string{"ns"}, nil))
	require.NoError(t, store.CreateNamespace(ctx, "default", []string{"ns2"}, nil))
}
