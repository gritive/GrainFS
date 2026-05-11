package volume

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gritive/GrainFS/internal/storage"
)

// s3SnapshotStore implements SnapshotStore using S3 live_map + snapshot
// namespace. This is the historical (dedup-disabled) implementation, moved
// here verbatim from Manager.
type s3SnapshotStore struct {
	m *Manager // back-reference for backend access and liveMap helpers
}

func newS3SnapshotStore(m *Manager) *s3SnapshotStore { return &s3SnapshotStore{m: m} }

func (s *s3SnapshotStore) CreateSnapshot(ctx context.Context, vol *Volume) (string, error) {
	m := s.m
	liveMap, err := m.getLiveMapUnlocked(vol.Name)
	if err != nil {
		return "", fmt.Errorf("load live_map: %w", err)
	}

	snapID := uuid.Must(uuid.NewV7()).String()

	snapMap := make(map[int64]string)
	if len(liveMap) == 0 {
		if err := m.backend.WalkObjects(ctx, volumeBucketName, blockPrefix(vol.Name), func(obj *storage.Object) error {
			blkNum, ok := parseBlockNum(obj.Key)
			if ok {
				snapMap[blkNum] = obj.Key
			}
			return nil
		}); err != nil {
			return "", fmt.Errorf("list blocks: %w", err)
		}
	} else {
		for k, v := range liveMap {
			snapMap[k] = v
		}
	}

	copier, hasCopier := m.backend.(storage.Copier)
	for blkNum, srcKey := range snapMap {
		dstKey := snapBlockKey(vol.Name, snapID, blkNum)
		if hasCopier {
			if _, err := copier.CopyObject(volumeBucketName, srcKey, volumeBucketName, dstKey); err != nil {
				return "", fmt.Errorf("copy block %d to snapshot: %w", blkNum, err)
			}
		} else {
			if err := m.copyObjectFallback(volumeBucketName, srcKey, volumeBucketName, dstKey); err != nil {
				return "", fmt.Errorf("copy block %d to snapshot: %w", blkNum, err)
			}
		}
		snapMap[blkNum] = dstKey
	}

	var mapBuf bytes.Buffer
	if err := serializeLiveMap(snapMap, &mapBuf); err != nil {
		return "", fmt.Errorf("serialize snapshot map: %w", err)
	}
	if _, err := m.backend.PutObject(ctx, volumeBucketName, snapMapKey(vol.Name, snapID), &mapBuf, "text/plain"); err != nil {
		return "", fmt.Errorf("store snapshot map: %w", err)
	}

	meta := snapshotMetaJSON{ID: snapID, CreatedAt: time.Now().UTC(), BlockCount: int64(len(snapMap))}
	metaData, err := json.Marshal(meta)
	if err != nil {
		return "", fmt.Errorf("marshal snapshot meta: %w", err)
	}
	if _, err := m.backend.PutObject(ctx, volumeBucketName, snapMetaKey(vol.Name, snapID), bytes.NewReader(metaData), "application/json"); err != nil {
		return "", fmt.Errorf("store snapshot meta: %w", err)
	}

	delete(m.liveMaps, vol.Name)
	return snapID, nil
}

func (s *s3SnapshotStore) ListSnapshots(ctx context.Context, name string) ([]SnapshotInfo, error) {
	m := s.m
	prefix := snapPrefix(name)
	var snaps []SnapshotInfo
	if err := m.backend.WalkObjects(ctx, volumeBucketName, prefix, func(obj *storage.Object) error {
		if !strings.HasSuffix(obj.Key, "/meta") {
			return nil
		}
		rc, _, err := m.backend.GetObject(ctx, volumeBucketName, obj.Key)
		if err != nil {
			return nil
		}
		data, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil
		}
		var meta snapshotMetaJSON
		if err := json.Unmarshal(data, &meta); err != nil {
			return nil
		}
		snaps = append(snaps, SnapshotInfo(meta))
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list snapshots: %w", err)
	}
	return snaps, nil
}

func (s *s3SnapshotStore) DeleteSnapshot(ctx context.Context, vol *Volume, snapID string) error {
	m := s.m
	rc, _, err := m.backend.GetObject(ctx, volumeBucketName, snapMapKey(vol.Name, snapID))
	if err != nil {
		return fmt.Errorf("snapshot %q not found for volume %q", snapID, vol.Name)
	}
	snapMap, err := parseLiveMap(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("parse snapshot map: %w", err)
	}
	for _, key := range snapMap {
		m.backend.DeleteObject(ctx, volumeBucketName, key) //nolint:errcheck
	}
	m.backend.DeleteObject(ctx, volumeBucketName, snapMapKey(vol.Name, snapID))  //nolint:errcheck
	m.backend.DeleteObject(ctx, volumeBucketName, snapMetaKey(vol.Name, snapID)) //nolint:errcheck
	return nil
}

func (s *s3SnapshotStore) Rollback(ctx context.Context, vol *Volume, snapID string) error {
	m := s.m
	rc, _, err := m.backend.GetObject(ctx, volumeBucketName, snapMapKey(vol.Name, snapID))
	if err != nil {
		return fmt.Errorf("snapshot %q not found for volume %q", snapID, vol.Name)
	}
	snapMap, err := parseLiveMap(rc)
	rc.Close()
	if err != nil {
		return fmt.Errorf("parse snapshot map: %w", err)
	}
	liveMap, err := m.getLiveMapUnlocked(vol.Name)
	if err != nil {
		return fmt.Errorf("load live_map: %w", err)
	}
	if liveMap == nil {
		liveMap = make(map[int64]string)
		m.liveMaps[vol.Name] = liveMap
	}
	copier, hasCopier := m.backend.(storage.Copier)
	for blkNum, snapKey := range snapMap {
		newKey := cowBlockKey(vol.Name, blkNum)
		if hasCopier {
			if _, err := copier.CopyObject(volumeBucketName, snapKey, volumeBucketName, newKey); err != nil {
				return fmt.Errorf("rollback block %d: %w", blkNum, err)
			}
		} else {
			if err := m.copyObjectFallback(volumeBucketName, snapKey, volumeBucketName, newKey); err != nil {
				return fmt.Errorf("rollback block %d: %w", blkNum, err)
			}
		}
		if oldKey, ok := liveMap[blkNum]; ok && oldKey != "" {
			m.backend.DeleteObject(ctx, volumeBucketName, oldKey) //nolint:errcheck
		}
		liveMap[blkNum] = newKey
	}
	for blkNum, oldKey := range liveMap {
		if _, inSnap := snapMap[blkNum]; !inSnap {
			m.backend.DeleteObject(ctx, volumeBucketName, oldKey) //nolint:errcheck
			delete(liveMap, blkNum)
		}
	}
	return m.persistLiveMapUnlocked(vol.Name, liveMap)
}

func (s *s3SnapshotStore) Clone(ctx context.Context, srcVol *Volume, dstName string) (*Volume, error) {
	m := s.m
	if err := m.ensureBucket(); err != nil {
		return nil, fmt.Errorf("ensure bucket: %w", err)
	}
	if _, _, err := m.backend.GetObject(ctx, volumeBucketName, metaKey(dstName)); err == nil {
		return nil, fmt.Errorf("volume %q already exists", dstName)
	}
	srcLiveMap, err := m.getLiveMapUnlocked(srcVol.Name)
	if err != nil {
		return nil, fmt.Errorf("load live_map: %w", err)
	}
	copier, hasCopier := m.backend.(storage.Copier)
	doCopy := func(srcKey, dstKey string) error {
		if hasCopier {
			_, err := copier.CopyObject(volumeBucketName, srcKey, volumeBucketName, dstKey)
			return err
		}
		return m.copyObjectFallback(volumeBucketName, srcKey, volumeBucketName, dstKey)
	}
	if len(srcLiveMap) > 0 {
		for blkNum, srcKey := range srcLiveMap {
			if err := doCopy(srcKey, blockKey(dstName, blkNum)); err != nil {
				return nil, fmt.Errorf("clone block %d: %w", blkNum, err)
			}
		}
	} else {
		if err := m.backend.WalkObjects(ctx, volumeBucketName, blockPrefix(srcVol.Name), func(obj *storage.Object) error {
			blkNum, ok := parseBlockNum(obj.Key)
			if !ok {
				return nil
			}
			return doCopy(obj.Key, blockKey(dstName, blkNum))
		}); err != nil {
			return nil, fmt.Errorf("clone blocks: %w", err)
		}
	}
	return &Volume{
		Name:            dstName,
		Size:            srcVol.Size,
		BlockSize:       srcVol.BlockSize,
		AllocatedBlocks: srcVol.AllocatedBlocks,
		SnapshotCount:   0,
	}, nil
}

func (s *s3SnapshotStore) RecoverOnBoot(ctx context.Context) error { return nil }
