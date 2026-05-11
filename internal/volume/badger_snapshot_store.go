package volume

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/volume/dedup"
)

// badgerSnapshotStore implements SnapshotStore using dedup-aware refcount
// sharing. All snapshot state (map + meta + in-progress markers) lives in
// BadgerDB; no S3 PutObject for meta (R3).
type badgerSnapshotStore struct {
	m   *Manager
	idx dedup.DedupIndex
}

func newBadgerSnapshotStore(m *Manager, idx dedup.DedupIndex) *badgerSnapshotStore {
	return &badgerSnapshotStore{m: m, idx: idx}
}

// CreateSnapshot iterates live vd:b:{vol}:* entries (R1: via IterLiveBlocks),
// appends chunked to vd:s:{vol}:{snapID}:, and commits atomically with meta.
func (s *badgerSnapshotStore) CreateSnapshot(ctx context.Context, vol *Volume) (string, error) {
	snapID := uuid.Must(uuid.NewV7()).String()

	if err := s.idx.SnapshotBegin(vol.Name, snapID); err != nil {
		return "", fmt.Errorf("snapshot begin: %w", err)
	}

	const chunk = 1024
	collected := make([]dedup.SnapshotBlockEntry, 0, chunk)
	var blockCount int64

	flush := func() error {
		if len(collected) == 0 {
			return nil
		}
		if err := s.idx.SnapshotAppendChunk(vol.Name, snapID, collected); err != nil {
			return err
		}
		collected = collected[:0]
		return nil
	}

	if err := s.idx.IterLiveBlocks(vol.Name, func(blkNum int64, canon string) error {
		collected = append(collected, dedup.SnapshotBlockEntry{BlkNum: blkNum, Canonical: canon})
		blockCount++
		if len(collected) >= chunk {
			return flush()
		}
		return nil
	}); err != nil {
		_, _ = s.idx.SnapshotAbort(vol.Name, snapID)
		return "", fmt.Errorf("iter live blocks: %w", err)
	}
	if err := flush(); err != nil {
		_, _ = s.idx.SnapshotAbort(vol.Name, snapID)
		return "", fmt.Errorf("append final chunk: %w", err)
	}

	meta := dedup.SnapshotMeta{
		SnapID:     snapID,
		CreatedAt:  time.Now().UTC(),
		BlockCount: blockCount,
	}
	if err := s.idx.SnapshotCommit(vol.Name, snapID, meta); err != nil {
		_, _ = s.idx.SnapshotAbort(vol.Name, snapID)
		return "", fmt.Errorf("commit: %w", err)
	}
	return snapID, nil
}

// ListSnapshots reads from BadgerDB (R3); no S3 WalkObjects.
func (s *badgerSnapshotStore) ListSnapshots(ctx context.Context, name string) ([]SnapshotInfo, error) {
	metas, err := s.idx.SnapshotList(name)
	if err != nil {
		return nil, fmt.Errorf("snapshot list: %w", err)
	}
	out := make([]SnapshotInfo, 0, len(metas))
	for _, m := range metas {
		out = append(out, SnapshotInfo{
			ID:         m.SnapID,
			CreatedAt:  m.CreatedAt,
			BlockCount: m.BlockCount,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (s *badgerSnapshotStore) DeleteSnapshot(ctx context.Context, vol *Volume, snapID string) error {
	toDelete, err := s.idx.SnapshotDelete(vol.Name, snapID)
	if err != nil {
		return fmt.Errorf("dedup snapshot delete: %w", err)
	}
	for _, key := range toDelete {
		s.m.backend.DeleteObject(ctx, volumeBucketName, key) //nolint:errcheck
	}
	return nil
}

func (s *badgerSnapshotStore) Rollback(ctx context.Context, vol *Volume, snapID string) error {
	toDelete, err := s.idx.SnapshotRollback(vol.Name, snapID)
	if err != nil {
		return fmt.Errorf("dedup snapshot rollback: %w", err)
	}
	for _, key := range toDelete {
		s.m.backend.DeleteObject(ctx, volumeBucketName, key) //nolint:errcheck
	}
	return nil
}

func (s *badgerSnapshotStore) Clone(ctx context.Context, srcVol *Volume, dstName string) (*Volume, error) {
	if err := s.m.ensureBucket(); err != nil {
		return nil, fmt.Errorf("ensure bucket: %w", err)
	}
	if _, _, err := s.m.backend.GetObject(ctx, volumeBucketName, metaKey(dstName)); err == nil {
		return nil, fmt.Errorf("volume %q already exists", dstName)
	}
	if err := s.idx.SnapshotClone(srcVol.Name, dstName); err != nil {
		return nil, fmt.Errorf("dedup clone: %w", err)
	}
	return &Volume{
		Name:            dstName,
		Size:            srcVol.Size,
		BlockSize:       srcVol.BlockSize,
		AllocatedBlocks: srcVol.AllocatedBlocks,
		SnapshotCount:   0,
	}, nil
}

// RecoverOnBoot (R2): aborts in-progress snapshots, then re-applies stuck
// rollback and clone markers. SnapshotRollback and SnapshotClone are
// chunked + idempotent, so re-calling them converges.
func (s *badgerSnapshotStore) RecoverOnBoot(ctx context.Context) error {
	inProg, err := s.idx.SnapshotListInProgress()
	if err != nil {
		return fmt.Errorf("list in-progress snapshots: %w", err)
	}
	for _, p := range inProg {
		toDelete, err := s.idx.SnapshotAbort(p.Vol, p.SnapID)
		if err != nil {
			return fmt.Errorf("abort %s/%s: %w", p.Vol, p.SnapID, err)
		}
		for _, key := range toDelete {
			s.m.backend.DeleteObject(ctx, volumeBucketName, key) //nolint:errcheck
		}
	}

	pendingRB, err := s.idx.SnapshotListPendingRollbacks()
	if err != nil {
		return fmt.Errorf("list pending rollbacks: %w", err)
	}
	for _, p := range pendingRB {
		toDelete, err := s.idx.SnapshotRollback(p.Vol, p.SnapID)
		if err != nil {
			return fmt.Errorf("recover rollback %s/%s: %w", p.Vol, p.SnapID, err)
		}
		for _, key := range toDelete {
			s.m.backend.DeleteObject(ctx, volumeBucketName, key) //nolint:errcheck
		}
	}

	pendingCl, err := s.idx.SnapshotListPendingClones()
	if err != nil {
		return fmt.Errorf("list pending clones: %w", err)
	}
	for _, p := range pendingCl {
		if err := s.idx.SnapshotClone(p.SrcVol, p.DstVol); err != nil {
			return fmt.Errorf("recover clone %s→%s: %w", p.SrcVol, p.DstVol, err)
		}
	}

	return nil
}
