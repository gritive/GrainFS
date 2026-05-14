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
	cascades  []string
	fsidMajor uint64
}

type recordingBarrier struct {
	indexes []uint64
	err     error
}

func (b *recordingBarrier) WaitApplied(_ context.Context, index uint64) error {
	b.indexes = append(b.indexes, index)
	return b.err
}

type indexedFakeProposer struct {
	store     *Store
	fsidMajor uint64
	nextIndex uint64
	upserts   []Config
	deletes   []string
}

func (p *indexedFakeProposer) ProposeUpsert(_ context.Context, bucket string, cfg Config) (uint64, error) {
	p.nextIndex++
	p.upserts = append(p.upserts, cfg)
	_, err := p.store.ApplyUpsert(bucket, cfg.ReadOnly, p.fsidMajor)
	return p.nextIndex, err
}

func (p *indexedFakeProposer) ProposeDelete(_ context.Context, bucket string) (uint64, error) {
	p.nextIndex++
	p.deletes = append(p.deletes, bucket)
	return p.nextIndex, p.store.Delete(bucket)
}

func (p *indexedFakeProposer) ProposeBucketDeleteCascade(_ context.Context, bucket string, _ bool) (uint64, error) {
	p.nextIndex++
	p.deletes = append(p.deletes, bucket)
	return p.nextIndex, p.store.Delete(bucket)
}

func (p *fakeProposer) ProposeUpsert(_ context.Context, bucket string, cfg Config) (uint64, error) {
	if p.err != nil {
		return 0, p.err
	}
	p.upserts = append(p.upserts, cfg)
	_, err := p.store.ApplyUpsert(bucket, cfg.ReadOnly, p.fsidMajor)
	return uint64(len(p.upserts) + len(p.deletes)), err
}

func (p *fakeProposer) ProposeDelete(_ context.Context, bucket string) (uint64, error) {
	if p.err != nil {
		return 0, p.err
	}
	p.deletes = append(p.deletes, bucket)
	return uint64(len(p.upserts) + len(p.deletes)), p.store.Delete(bucket)
}

func (p *fakeProposer) ProposeBucketDeleteCascade(_ context.Context, bucket string, _ bool) (uint64, error) {
	if p.err != nil {
		return 0, p.err
	}
	p.cascades = append(p.cascades, bucket)
	return uint64(len(p.upserts) + len(p.deletes) + len(p.cascades)), p.store.Delete(bucket)
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

func TestExportServiceWaitsForCommittedIndex(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()
	p := &indexedFakeProposer{store: store, fsidMajor: 7}
	barrier := &recordingBarrier{}
	svc := NewExportService(ServiceConfig{Store: store, Proposer: p, Barrier: barrier})

	require.NoError(t, svc.Upsert(context.Background(), "b1", UpsertParams{}))
	require.Equal(t, []uint64{1}, barrier.indexes)
	require.NoError(t, svc.Delete(context.Background(), "b1"))
	require.Equal(t, []uint64{1, 2}, barrier.indexes)
}

func TestExportServiceWrapsPropagationBarrierError(t *testing.T) {
	db, store := openTestStore(t, t.TempDir())
	defer db.Close()
	p := &indexedFakeProposer{store: store, fsidMajor: 7}
	barrier := &recordingBarrier{err: context.DeadlineExceeded}
	svc := NewExportService(ServiceConfig{Store: store, Proposer: p, Barrier: barrier})

	err := svc.Upsert(context.Background(), "b1", UpsertParams{})
	require.ErrorIs(t, err, ErrPropagationTimeout)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
