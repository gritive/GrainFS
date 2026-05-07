package cluster

import (
	"context"
	"errors"
	"fmt"

	"github.com/gritive/GrainFS/internal/storage"
)

type ObjectIndexIssueKind string

const (
	ObjectIndexIssueOrphan ObjectIndexIssueKind = "orphan"
	ObjectIndexIssueStale  ObjectIndexIssueKind = "stale"
)

type ObjectIndexIssue struct {
	Kind             ObjectIndexIssueKind
	Bucket           string
	Key              string
	VersionID        string
	PlacementGroupID string
	Reason           string
}

// ReconcileObjectIndexLatest checks the global object index row for one object
// against the group-local metadata it points at. It is intentionally detection
// first: callers can surface the issue without treating a dual-write mismatch
// as a successful object operation.
func (c *ClusterCoordinator) ReconcileObjectIndexLatest(ctx context.Context, bucket, key string) ([]ObjectIndexIssue, error) {
	target, entry, err := c.routeObjectLatest(bucket, key)
	if err != nil {
		return nil, err
	}
	obj, err := c.headObjectVersionAt(ctx, target, bucket, key, entry.VersionID)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return []ObjectIndexIssue{{
				Kind:             ObjectIndexIssueStale,
				Bucket:           bucket,
				Key:              key,
				VersionID:        entry.VersionID,
				PlacementGroupID: entry.PlacementGroupID,
				Reason:           "index points at missing data-group object",
			}}, nil
		}
		return nil, err
	}
	if obj.Size != entry.Size || obj.ETag != entry.ETag {
		return []ObjectIndexIssue{{
			Kind:             ObjectIndexIssueStale,
			Bucket:           bucket,
			Key:              key,
			VersionID:        entry.VersionID,
			PlacementGroupID: entry.PlacementGroupID,
			Reason:           fmt.Sprintf("metadata mismatch size=%d/%d etag=%q/%q", obj.Size, entry.Size, obj.ETag, entry.ETag),
		}}, nil
	}
	return nil, nil
}

// FindObjectIndexOrphans scans group-local object metadata and reports versions
// that are not represented in the global object index.
func (c *ClusterCoordinator) FindObjectIndexOrphans(ctx context.Context) ([]ObjectIndexIssue, error) {
	src, ok := c.meta.(objectIndexSource)
	if !ok {
		return nil, ErrObjectIndexRequired
	}
	objects, err := c.ListAllObjects()
	if err != nil {
		return nil, err
	}
	issues := make([]ObjectIndexIssue, 0)
	for _, obj := range objects {
		if obj.IsDeleteMarker {
			continue
		}
		if _, ok := src.ObjectIndexVersion(obj.Bucket, obj.Key, obj.VersionID); ok {
			continue
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		issues = append(issues, ObjectIndexIssue{
			Kind:      ObjectIndexIssueOrphan,
			Bucket:    obj.Bucket,
			Key:       obj.Key,
			VersionID: obj.VersionID,
			Reason:    "data-group object has no global object-index row",
		})
	}
	return issues, nil
}

func (c *ClusterCoordinator) headObjectVersionAt(ctx context.Context, target routeTarget, bucket, key, versionID string) (*storage.Object, error) {
	if gb, ok, err := c.localReadBackend(ctx, target); ok {
		if err != nil {
			return nil, err
		}
		return gb.HeadObjectVersion(bucket, key, versionID)
	}
	if c.forward == nil {
		return nil, ErrCoordinatorNoRouter
	}
	rc, obj, err := c.GetObjectVersion(bucket, key, versionID)
	if err != nil {
		return nil, err
	}
	_ = rc.Close()
	return obj, nil
}
