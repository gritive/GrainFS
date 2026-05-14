package nfsexport

import (
	"context"
	"errors"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

type fakeProposer struct {
	store     *Store
	err       error
	upserts   []Config
	deletes   []string
	fsidMajor uint64
}

func (p *fakeProposer) ProposeUpsert(_ context.Context, bucket string, cfg Config) error {
	if p.err != nil {
		return p.err
	}
	p.upserts = append(p.upserts, cfg)
	_, err := p.store.ApplyUpsert(bucket, cfg.ReadOnly, p.fsidMajor)
	return err
}

func (p *fakeProposer) ProposeDelete(_ context.Context, bucket string) error {
	if p.err != nil {
		return p.err
	}
	p.deletes = append(p.deletes, bucket)
	return p.store.Delete(bucket)
}

func newTestService(t *testing.T) (*badger.DB, *Store, *fakeProposer, *ExportService) {
	t.Helper()
	db, store := openTestStore(t, t.TempDir())
	p := &fakeProposer{store: store, fsidMajor: 7}
	svc := NewExportService(ServiceConfig{Store: store, Proposer: p})
	return db, store, p, svc
}

func TestExportServiceUpsertAssignsMinorAndGeneration(t *testing.T) {
	db, _, p, svc := newTestService(t)
	defer db.Close()

	require.NoError(t, svc.Upsert(context.Background(), "b1", UpsertParams{}))
	cfg, ok := svc.Get("b1")
	require.True(t, ok)
	require.Equal(t, uint64(7), cfg.FsidMajor)
	require.NotZero(t, cfg.FsidMinor)
	require.Equal(t, uint64(1), cfg.Generation)
	require.Equal(t, Config{}, p.upserts[0])
}

func TestExportServiceUpsertKeepsMinorAndBumpsGeneration(t *testing.T) {
	db, _, _, svc := newTestService(t)
	defer db.Close()

	require.NoError(t, svc.Upsert(context.Background(), "b1", UpsertParams{}))
	first, ok := svc.Get("b1")
	require.True(t, ok)
	require.NoError(t, svc.Upsert(context.Background(), "b1", UpsertParams{ReadOnly: true}))
	second, ok := svc.Get("b1")
	require.True(t, ok)
	require.Equal(t, first.FsidMinor, second.FsidMinor)
	require.Equal(t, uint64(2), second.Generation)
	require.True(t, second.ReadOnly)
}

func TestExportServiceUpsertDistinctBucketsGetDistinctMinors(t *testing.T) {
	db, _, _, svc := newTestService(t)
	defer db.Close()

	require.NoError(t, svc.Upsert(context.Background(), "b1", UpsertParams{}))
	require.NoError(t, svc.Upsert(context.Background(), "b2", UpsertParams{}))
	first, _ := svc.Get("b1")
	second, _ := svc.Get("b2")
	require.NotEqual(t, first.FsidMinor, second.FsidMinor)
	require.Equal(t, []string{"b1", "b2"}, svc.List())
}

func TestExportServiceDeleteIdempotent(t *testing.T) {
	db, _, p, svc := newTestService(t)
	defer db.Close()

	require.NoError(t, svc.Upsert(context.Background(), "b1", UpsertParams{}))
	require.NoError(t, svc.Delete(context.Background(), "b1"))
	require.NoError(t, svc.Delete(context.Background(), "b1"))
	require.Equal(t, []string{"b1", "b1"}, p.deletes)
	_, ok := svc.Get("b1")
	require.False(t, ok)
}

func TestExportServiceProposerErrorPropagates(t *testing.T) {
	db, _, p, svc := newTestService(t)
	defer db.Close()

	want := errors.New("propose failed")
	p.err = want
	require.ErrorIs(t, svc.Upsert(context.Background(), "b1", UpsertParams{}), want)
}

func TestExportServiceRejectsMultiNodeWithoutPropagationBarrier(t *testing.T) {
	db, store, p, _ := newTestService(t)
	defer db.Close()
	svc := NewExportService(ServiceConfig{
		Store:            store,
		Proposer:         p,
		ClusterNodeCount: func() int { return 2 },
	})

	require.ErrorIs(t, svc.Upsert(context.Background(), "b1", UpsertParams{}), ErrPropagationBarrierRequired)
	require.Empty(t, p.upserts)

	require.ErrorIs(t, svc.Delete(context.Background(), "b1"), ErrPropagationBarrierRequired)
	require.Empty(t, p.deletes)
}
