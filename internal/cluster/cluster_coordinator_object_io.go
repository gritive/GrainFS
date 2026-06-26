package cluster

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/gritive/GrainFS/internal/raft/raftpb"
	"github.com/gritive/GrainFS/internal/storage"
	"github.com/rs/zerolog/log"
)

func contextWithObjectWritePlacement(ctx context.Context, group ShardGroupEntry) context.Context {
	if len(group.PeerIDs) == 0 {
		return ContextWithPlacementGroup(ctx, group.ID)
	}
	return ContextWithPlacementGroupEntry(ctx, group)
}

type bucketVersionedCtxKey struct{}

// ContextWithBucketVersioning stamps an authoritative versioning decision for
// this PUT. The S3 handler resolves the bucket versioning state at the edge
// (the same place AppendObject reads it) and ALWAYS stamps it — both enabled
// and disabled — so the per-group commit backend never has to read bucket
// versioning itself. That read would be a control/data-plane boundary
// violation (the commit backend has no replicated bucketver state). The
// decision also rides the forward wire (PutObjectArgs.versioning_state) so a
// forwarded PUT received by another node carries the same authoritative value.
func ContextWithBucketVersioning(ctx context.Context, enabled bool) context.Context {
	return context.WithValue(ctx, bucketVersionedCtxKey{}, enabled)
}

// bucketVersioningFromContext returns the stamped versioning decision and
// whether it was resolved. resolved is false only when no edge/forward layer
// stamped the context (e.g. an in-process DistributedBackend test that bypasses
// the coordinator), in which case the caller falls back to a local read.
func bucketVersioningFromContext(ctx context.Context) (enabled bool, resolved bool) {
	v, ok := ctx.Value(bucketVersionedCtxKey{}).(bool)
	return v, ok
}

// BucketVersioningFromContext is the exported accessor for the stamped
// versioning decision. Used by the server edge (and tests) to inspect whether
// the authoritative versioning flag was threaded into ctx.
func BucketVersioningFromContext(ctx context.Context) (enabled bool, resolved bool) {
	return bucketVersioningFromContext(ctx)
}

func topologyForwardWriteError(group ShardGroupEntry, err error) error {
	if err == nil || !errors.Is(err, ErrNoReachablePeer) || len(group.PeerIDs) == 0 {
		return err
	}
	cfg := DesiredECConfigForGroup(group)
	if cfg.NumShards() == 0 || len(group.PeerIDs) < cfg.NumShards() {
		return err
	}
	return &ErrInsufficientPlacementTargets{
		Operation:     "put_object",
		GroupID:       group.ID,
		Desired:       cfg,
		Configured:    cloneStringSlice(group.PeerIDs),
		Unavailable:   cloneStringSlice(group.PeerIDs),
		FailureReason: fmt.Sprintf("forward target unavailable: %v", err),
	}
}

func logForwardReplyDecodeError(err error, bucket, key, groupID string, op raftpb.ForwardOp, reply []byte) {
	log.Warn().
		Err(err).
		Str("bucket", bucket).
		Str("key", key).
		Str("group_id", groupID).
		Str("op", op.String()).
		Str("forward_status", forwardReplyStatusString(op, reply)).
		Bool("has_object", forwardReplyHasObject(reply)).
		Int("reply_bytes", len(reply)).
		Msg("forward: decode reply failed")
}

// GetObject reads the object body and metadata. Production forwarding streams
// response body bytes after the metadata reply; legacy tests can still use the
// single-frame read_body fallback.
func (c *ClusterCoordinator) GetObject(ctx context.Context, bucket, key string) (io.ReadCloser, *storage.Object, error) {
	// S4-4c: index-free. ResolveRead serves locally only when this node can
	// answer authoritatively (sole voter, internal bucket, or a leader doing a
	// linearizable ReadIndex); otherwise it returns nil and we forward to the
	// placement-group leader. The GroupBackend resolves object metadata from
	// quorum meta (local file → peer fan-out) and folds a delete-marker latest
	// version to ErrObjectNotFound, so the unversioned-GET 404 semantics that the
	// object index used to short-circuit are now served by the backend read.
	// S7-4: probeRead tries each topology generation newest-first; at a single
	// generation (the default) it is one attempt against the same target.
	var (
		rc  io.ReadCloser
		obj *storage.Object
	)
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			r, o, e := gb.GetObject(ctx, bucket, key)
			if e != nil {
				return e
			}
			rc, obj = r, o
			return nil
		}
		args := buildGetObjectArgs(bucket, key, versioningStateFromContext(ctx))
		r, o, e := c.forwardRuntime().readObject(ctx, target, raftpb.ForwardOpGetObject, args)
		if e != nil {
			return e
		}
		rc, obj = r, o
		return nil
	})
	return rc, obj, err
}

func (c *ClusterCoordinator) GetObjectVersion(
	ctx context.Context, bucket, key, versionID string,
) (io.ReadCloser, *storage.Object, error) {
	// S4-4c: index-free (see GetObject). Local serve only when authoritative;
	// otherwise forward to the placement-group leader.
	// S7-4c: a specific versionID lives in exactly one generation; probeRead
	// walks generations newest-first until the version is found (advancing only
	// on not-found, fail-closed otherwise). Single generation → one attempt.
	var (
		rc  io.ReadCloser
		obj *storage.Object
	)
	err := c.probeRead(bucket, key, versionID, func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			r, o, e := gb.getObjectVersionCtx(ctx, bucket, key, versionID)
			if e != nil {
				return e
			}
			rc, obj = r, o
			return nil
		}
		args := buildGetObjectVersionArgs(bucket, key, versionID, versioningStateFromContext(ctx))
		r, o, e := c.forwardRuntime().readObject(ctx, target, raftpb.ForwardOpGetObjectVersion, args)
		if e != nil {
			return e
		}
		rc, obj = r, o
		return nil
	})
	return rc, obj, err
}

func (c *ClusterCoordinator) HeadObject(ctx context.Context, bucket, key string) (*storage.Object, error) {
	// S4-4c: index-free (see GetObject). The GroupBackend HeadObject folds a
	// delete-marker latest version to ErrObjectNotFound, preserving the
	// unversioned-HEAD 404 semantics the object index used to short-circuit.
	// S7-4: probeRead tries each topology generation newest-first; one attempt
	// at a single generation (the default).
	var obj *storage.Object
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			o, e := gb.HeadObject(ctx, bucket, key)
			if e != nil {
				return e
			}
			obj = o
			return nil
		}
		args := buildHeadObjectArgs(bucket, key, versioningStateFromContext(ctx))
		o, e := c.forwardRuntime().headObject(ctx, target, raftpb.ForwardOpHeadObject, args, bucket, key)
		if e != nil {
			return e
		}
		obj = o
		return nil
	})
	return obj, err
}

func (c *ClusterCoordinator) HeadObjectVersion(ctx context.Context, bucket, key, versionID string) (*storage.Object, error) {
	// S7-4c: probe generations newest-first for the specific version (one
	// attempt at a single generation).
	var obj *storage.Object
	err := c.probeRead(bucket, key, versionID, func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			o, e := gb.headObjectVersionCtx(ctx, bucket, key, versionID)
			if e != nil {
				return e
			}
			obj = o
			return nil
		}
		args := buildHeadObjectVersionArgs(bucket, key, versionID, versioningStateFromContext(ctx))
		o, e := c.forwardRuntime().headObject(ctx, target, raftpb.ForwardOpHeadObjectVersion, args, bucket, key)
		if e != nil {
			return e
		}
		obj = o
		return nil
	})
	return obj, err
}

func (c *ClusterCoordinator) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := c.DeleteObjectReturningMarker(bucket, key)
	return err
}

func (c *ClusterCoordinator) DeleteObjectReturningMarker(bucket, key string) (string, error) {
	ctx := context.Background()
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return "", err
	}
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return "", err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return "", err
	} else if gb != nil {
		return gb.DeleteObjectReturningMarker(bucket, key)
	}
	args := buildDeleteObjectArgs(bucket, key)
	return c.forwardRuntime().deleteObject(ctx, target, args)
}

func (c *ClusterCoordinator) DeleteObjectVersion(bucket, key, versionID string) error {
	ctx := context.Background()
	targets, err := c.routeReadGenerations(bucket, key, versionID)
	if err != nil {
		return err
	}
	// A version record lives in exactly one generation group — whichever the key
	// hashed to when that version was written — but routing cannot tell which
	// without reading. The per-version FSM delete is idempotent (no-op when the
	// version is absent), so we cannot use probeRead's stop-on-first-success loop
	// (the newest-gen group would "succeed" by no-op and we'd never reach the
	// resident older-gen group). Instead fan the delete out to every generation
	// group: the resident group deletes the record, the rest no-op idempotently
	// (a group that holds neither this version nor a stale lat: pointer to it does
	// nothing; the per-group latest-recompute is local and harmless).
	// Dedup repeated group IDs (a key may hash to the same group across
	// generations) and fail-closed on the first real error so the client retries
	// the whole (idempotent) fan-out rather than leaving the record behind in a
	// transiently-unreachable group.
	seen := make(map[string]struct{}, len(targets))
	var firstErr error
	for _, target := range targets {
		if _, dup := seen[target.GroupID]; dup {
			continue
		}
		seen[target.GroupID] = struct{}{}
		if derr := c.deleteObjectVersionOnTarget(ctx, target, bucket, key, versionID); derr != nil && firstErr == nil {
			firstErr = derr
		}
	}
	return firstErr
}

func (c *ClusterCoordinator) deleteObjectVersionOnTarget(ctx context.Context, target RouteTarget, bucket, key, versionID string) error {
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.DeleteObjectVersion(bucket, key, versionID)
	}
	args := buildDeleteObjectVersionArgs(bucket, key, versionID)
	return c.forwardRuntime().mutateFrame(ctx, target, raftpb.ForwardOpDeleteObjectVersion, args)
}

func (c *ClusterCoordinator) ListObjects(ctx context.Context, bucket, prefix string, maxKeys int) ([]*storage.Object, error) {
	objects, _, err := c.ListObjectsPage(ctx, bucket, prefix, "", maxKeys)
	return objects, err
}

// ListObjectsPage returns one S3 ListObjects page. Entries with key > marker
// are returned, up to maxKeys. truncated reports whether more entries match
// beyond the returned slice — the S3 handler maps this to IsTruncated and
// NextMarker.
func (c *ClusterCoordinator) ListObjectsPage(ctx context.Context, bucket, prefix, marker string, maxKeys int) (objects []*storage.Object, truncated bool, err error) {
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, false, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, false, err
	} else if gb != nil {
		return gb.ListObjectsPage(ctx, bucket, prefix, marker, maxKeys)
	}
	return c.forwardRuntime().listObjects(ctx, target, bucket, prefix, marker, maxKeys)
}

// WalkObjects buffers ALL matching objects on the server and returns them in
// one reply. Callers expecting large keysets should use ListObjects with
// maxKeys pagination instead.
func (c *ClusterCoordinator) WalkObjects(ctx context.Context, bucket, prefix string, fn func(*storage.Object) error) error {
	target, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.WalkObjects(ctx, bucket, prefix, fn)
	}
	return c.forwardRuntime().walkObjects(ctx, target, bucket, prefix, fn)
}

func (c *ClusterCoordinator) PutObject(
	ctx context.Context, bucket, key string, r io.Reader, contentType string,
) (*storage.Object, error) {
	return c.PutObjectWithUserMetadata(ctx, bucket, key, r, contentType, nil)
}

func (c *ClusterCoordinator) PutObjectWithUserMetadata(
	ctx context.Context, bucket, key string, r io.Reader, contentType string, userMetadata map[string]string,
) (*storage.Object, error) {
	return c.PutObjectWithRequest(ctx, storage.PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (c *ClusterCoordinator) PutObjectWithRequest(ctx context.Context, req storage.PutObjectRequest) (*storage.Object, error) {
	bucket, key, r := req.Bucket, req.Key, req.Body
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	c.ensureGenZero(ctx)
	routeStart := time.Now()
	target, group, err := c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return nil, err
	}
	sizeClass := PutTraceSizeUnknown
	if s, ok := r.(interface{ Len() int }); ok {
		sizeClass = putTraceSizeClass(int64(s.Len()), c.maxBody)
	}
	ctx = contextWithObjectWritePlacement(ctx, group)
	if gb, err := c.runtimeState().localExec.ResolveObjectWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		ctx = ContextWithPutTrace(ctx, PutTraceRequest{
			Bucket:      bucket,
			Key:         key,
			GroupID:     group.ID,
			Ingress:     PutTraceIngressLocalLeader,
			SizeClass:   sizeClass,
			ForwardMode: PutTraceForwardNone,
		})
		ObservePutTraceStage(ctx, PutTraceStageRouteWrite, routeStart, PutTraceStageFields{})
		// Single path: every local PUT goes through PutObjectWithRequest (the
		// other PutObject* methods are thin wrappers around it), and the forward
		// path below now carries the same user metadata, so a PUT has the same
		// effect on a voter or a non-voter node.
		obj, err := gb.PutObjectWithRequest(ctx, req)
		if err != nil {
			return nil, err
		}
		return obj, nil
	}
	return c.forwardRuntime().putObject(ctx, target, group, req, routeStart)
}

func (c *ClusterCoordinator) PutObjectWithUserMetadataResult(
	ctx context.Context,
	bucket, key string,
	r io.Reader,
	contentType string,
	userMetadata map[string]string,
) (*storage.PutObjectResult, error) {
	return c.PutObjectWithRequestResult(ctx, storage.PutObjectRequest{
		Bucket:       bucket,
		Key:          key,
		Body:         r,
		ContentType:  contentType,
		UserMetadata: userMetadata,
	})
}

func (c *ClusterCoordinator) PutObjectWithRequestResult(ctx context.Context, req storage.PutObjectRequest) (*storage.PutObjectResult, error) {
	previous, err := c.previousObjectForMutation(ctx, req.Bucket, req.Key)
	if err != nil {
		log.Warn().
			Err(err).
			Str("bucket", req.Bucket).
			Str("key", req.Key).
			Bool("has_sse", req.SystemMetadata.SSEAlgorithm != "").
			Msg("coordinator put: previous object lookup failed")
		return nil, err
	}
	obj, err := c.PutObjectWithRequest(ctx, req)
	if err != nil {
		log.Warn().
			Err(err).
			Str("bucket", req.Bucket).
			Str("key", req.Key).
			Bool("has_sse", req.SystemMetadata.SSEAlgorithm != "").
			Msg("coordinator put: write failed")
		return nil, err
	}
	facts, err := objectFactsForMutation("PutObject", obj)
	if err != nil {
		return nil, err
	}
	return &storage.PutObjectResult{Object: facts, Previous: previous}, nil
}

func (c *ClusterCoordinator) previousObjectForMutation(ctx context.Context, bucket, key string) (storage.PreviousObject, error) {
	obj, err := c.HeadObject(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return storage.PreviousObject{}, nil
		}
		return storage.PreviousObject{}, err
	}
	return previousObjectFacts(obj)
}

func previousObjectFacts(obj *storage.Object) (storage.PreviousObject, error) {
	if obj == nil {
		return storage.PreviousObject{}, storage.InvalidMutationResultError{Op: "HeadObject", Field: "object", Reason: "nil object"}
	}
	if obj.Size < 0 {
		return storage.PreviousObject{}, storage.InvalidMutationResultError{Op: "HeadObject", Field: "size", Reason: "negative size"}
	}
	return storage.PreviousObject{
		Exists:    true,
		Size:      obj.Size,
		ETag:      obj.ETag,
		VersionID: obj.VersionID,
	}, nil
}

func objectFactsForMutation(op string, obj *storage.Object) (storage.ObjectFacts, error) {
	if obj == nil {
		return storage.ObjectFacts{}, storage.InvalidMutationResultError{Op: op, Field: "object", Reason: "nil object"}
	}
	if obj.Size < 0 {
		return storage.ObjectFacts{}, storage.InvalidMutationResultError{Op: op, Field: "size", Reason: "negative size"}
	}
	if obj.ETag == "" {
		return storage.ObjectFacts{}, storage.InvalidMutationResultError{Op: op, Field: "etag", Reason: "empty etag"}
	}
	return storage.ObjectFacts{
		Size:         obj.Size,
		ETag:         obj.ETag,
		VersionID:    obj.VersionID,
		LastModified: obj.LastModified,
		SSEAlgorithm: obj.SSEAlgorithm,
	}, nil
}

func (c *ClusterCoordinator) PutObjectWithACL(
	bucket, key string, r io.Reader, contentType string, acl uint8,
) (*storage.Object, error) {
	ctx := context.Background()
	obj, err := c.PutObject(ctx, bucket, key, r, contentType)
	if err != nil {
		return nil, err
	}
	if err := c.SetObjectACL(bucket, key, acl); err != nil {
		if obj == nil || obj.VersionID == "" {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: missing version id",
				storage.UnsupportedOperationError{Op: "PutObjectWithACL", Reason: storage.UnsupportedReasonRollbackFailed},
				err,
			)
		}
		if rollbackErr := c.DeleteObjectVersion(bucket, key, obj.VersionID); rollbackErr != nil {
			return nil, fmt.Errorf("%w: acl error: %v; rollback error: %v",
				storage.UnsupportedOperationError{Op: "PutObjectWithACL", Reason: storage.UnsupportedReasonRollbackFailed},
				err,
				rollbackErr,
			)
		}
		return nil, err
	}
	obj.ACL = acl
	return obj, nil
}

func (c *ClusterCoordinator) SetObjectACL(bucket, key string, acl uint8) error {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		// S4-4c: index-free. The ACL change is a read-modify-write against the
		// quorum-meta file (Phase 3); the old index-gated propose path is gone.
		return gb.SetObjectACL(bucket, key, acl)
	}
	args := buildSetObjectACLArgs(bucket, key, acl)
	return c.forwardRuntime().mutateFrame(ctx, target, raftpb.ForwardOpSetObjectACL, args)
}

// SetObjectTags satisfies storage.ObjectTagsSetter. Routes the tag write
// either to the locally-resolvable group backend (if self is leader) or
// forwards to the owning peer via ForwardOpSetObjectTags. Mirrors SetObjectACL.
func (c *ClusterCoordinator) SetObjectTags(bucket, key, versionID string, tags []storage.Tag) error {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		// S4-4c: index-free. Tag write is a read-modify-write against the
		// quorum-meta file (Phase 3); the old index-gated propose path is gone.
		return gb.SetObjectTags(bucket, key, versionID, tags)
	}
	args := buildSetObjectTagsArgs(bucket, key, versionID, tags)
	return c.forwardRuntime().mutateFrame(ctx, target, raftpb.ForwardOpSetObjectTags, args)
}

// QuarantineObject routes the quarantine operation to the owning group backend
// (local exec if self is leader) or forwards to the owning peer.
func (c *ClusterCoordinator) QuarantineObject(ctx context.Context, bucket, key, versionID, cause, reason string) error {
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return err
	}
	if gb, err := c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.QuarantineObject(ctx, bucket, key, versionID, cause, reason)
	}
	args := buildSetObjectQuarantineArgs(bucket, key, versionID, cause, reason)
	return c.forwardRuntime().mutateFrame(ctx, target, raftpb.ForwardOpSetObjectQuarantine, args)
}

// GetObjectTags satisfies storage.ObjectTagsGetter. Routes the tag read to
// the locally-resolvable group backend when available, otherwise forwards to
// the owning peer via ForwardOpGetObjectTags. Mirrors SetObjectTags's forward
// path so multi-group cluster deployments serve S3 GetObjectTagging instead
// of erroring with "not implemented".
func (c *ClusterCoordinator) GetObjectTags(bucket, key, versionID string) ([]storage.Tag, error) {
	ctx := context.Background()
	target, err := c.routeReadOrBucket(bucket, key, "")
	if err != nil {
		return nil, err
	}
	if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.GetObjectTags(bucket, key, versionID)
	}
	return c.forwardRuntime().getObjectTags(ctx, target, bucket, key, versionID)
}

// WriteAt implements the pwrite fast path for routed internal buckets such as
// NFSv4. Uses the generic RMW path (GetObject -> modify -> PutObject) which
// works correctly for both encrypted and unencrypted backends.
func (c *ClusterCoordinator) WriteAt(ctx context.Context, bucket, key string, offset uint64, data []byte) (*storage.Object, error) {
	_, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return nil, err
	}

	var existing []byte
	rc, _, err := c.GetObject(ctx, bucket, key)
	if err == nil {
		existing, err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return nil, err
		}
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return nil, err
	}

	end := offset + uint64(len(data))
	if end < offset {
		return nil, storage.ErrEntityTooLarge
	}
	if uint64(len(existing)) < end {
		existing = append(existing, make([]byte, end-uint64(len(existing)))...)
	}
	copy(existing[offset:], data)
	return c.PutObject(ctx, bucket, key, bytes.NewReader(existing), "application/octet-stream")
}

// Truncate implements the SETATTR-size path for internal buckets via a generic
// RMW (GetObject -> resize -> PutObject), which works correctly for both
// encrypted and unencrypted backends.
func (c *ClusterCoordinator) Truncate(ctx context.Context, bucket, key string, size int64) error {
	if size < 0 {
		return storage.ErrEntityTooLarge
	}
	_, err := c.runtimeState().opRouter.RouteBucket(bucket)
	if err != nil {
		return err
	}

	var existing []byte
	rc, _, err := c.GetObject(ctx, bucket, key)
	if err == nil {
		existing, err = io.ReadAll(rc)
		rc.Close()
		if err != nil {
			return err
		}
	} else if !errors.Is(err, storage.ErrObjectNotFound) {
		return err
	}
	cur := int64(len(existing))
	switch {
	case cur > size:
		existing = existing[:size]
	case cur < size:
		existing = append(existing, make([]byte, size-cur)...)
	}
	_, err = c.PutObject(ctx, bucket, key, bytes.NewReader(existing), "application/octet-stream")
	return err
}

// ReadAt implements the pread fast path. Local leaders use the group backend's
// zero-copy path. Follower voters may serve immutable object reads locally only
// after their local metadata matches the cluster entry; stale followers still
// forward to the leader. Internal buckets are rejected at the backend core layer
// (guardInternalBucketObjectOp) before reaching the storage read path.
func (c *ClusterCoordinator) ReadAt(ctx context.Context, bucket, key string, offset int64, buf []byte) (int, error) {
	if offset < 0 {
		return 0, errors.New("coordinator: negative ReadAt offset")
	}
	// S7-4c: probe generations newest-first (one attempt at a single
	// generation). The no-readDialer fallback delegates to GetObject, which
	// probes on its own; the local and forward-readAt paths route per target.
	var n int
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		if gb, err := c.runtimeState().localExec.ResolveRead(ctx, target); err != nil {
			return err
		} else if gb != nil {
			var e error
			n, e = gb.ReadAt(ctx, bucket, key, offset, buf)
			return e
		}
		if c.forward == nil {
			return ErrCoordinatorNoRouter
		}
		if c.forward.readDialer == nil {
			rc, _, e := c.GetObject(ctx, bucket, key)
			if e != nil {
				return e
			}
			defer rc.Close()
			if _, e := io.CopyN(io.Discard, rc, offset); e != nil {
				return e
			}
			n, e = io.ReadFull(rc, buf)
			return e
		}
		args := buildReadAtArgs(bucket, key, offset, int64(len(buf)))
		var e error
		n, e = c.forwardRuntime().readAt(ctx, target, args, buf)
		return e
	})
	return n, err
}

func (c *ClusterCoordinator) ReadAtObject(ctx context.Context, bucket, key string, obj *storage.Object, offset int64, buf []byte) (int, error) {
	if obj == nil {
		return c.ReadAt(ctx, bucket, key, offset, buf)
	}
	if obj.Key != "" && obj.Key != key {
		return 0, fmt.Errorf("coordinator: ReadAt object key mismatch: got %q, want %q", obj.Key, key)
	}
	if offset < 0 {
		return 0, errors.New("coordinator: negative ReadAt offset")
	}
	// S7-4c: probe generations newest-first; non-authoritative falls back to
	// ReadAt (which probes too). One attempt at a single generation.
	var n int
	err := c.probeRead(bucket, key, "", func(target RouteTarget) error {
		gb, rerr := c.runtimeState().localExec.ResolveRead(ctx, target)
		if rerr != nil {
			return rerr
		}
		if gb == nil {
			var e error
			n, e = c.ReadAt(ctx, bucket, key, offset, buf)
			return e
		}
		var e error
		n, e = gb.ReadAtObject(ctx, bucket, key, obj, offset, buf)
		return e
	})
	return n, err
}

func (c *ClusterCoordinator) PreferReadAt(bucket string) bool {
	return true
}

// PreferWriteAt always returns false. The plain-file pwrite fast-path has been
// removed; all internal-bucket writes now use the encrypted RMW path via PutObject.
func (c *ClusterCoordinator) PreferWriteAt(bucket string) bool {
	return false
}

func (c *ClusterCoordinator) UploadPart(
	ctx context.Context, bucket, key, uploadID string, partNumber int, r io.Reader, contentMD5Hex string,
) (*storage.Part, error) {
	return c.multipartRuntime().uploadPart(ctx, bucket, key, uploadID, partNumber, r, contentMD5Hex)
}

type forwardBodyBytesProvider interface {
	ForwardBodyBytes() []byte
}

func forwardBodyBytes(r io.Reader, maxBody int64) ([]byte, error) {
	if provider, ok := r.(forwardBodyBytesProvider); ok {
		body := provider.ForwardBodyBytes()
		if int64(len(body)) > maxBody {
			return nil, storage.ErrEntityTooLarge
		}
		return body, nil
	}
	return readBoundedBody(r, maxBody)
}

func readBoundedBody(r io.Reader, maxBody int64) ([]byte, error) {
	// Forward-frame and retry boundaries need a replayable body. Keep this
	// allocation explicit, capped, and shared so ReadAll cannot creep into
	// unbounded hot paths.
	body, err := io.ReadAll(io.LimitReader(r, maxBody+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maxBody {
		return nil, storage.ErrEntityTooLarge
	}
	return body, nil
}

// AppendObject implements storage.AppendObjecter at the cluster-coordinator
// level. It routes the append to the owner shard group:
//   - local path: dispatches into GroupBackend.AppendObject (DistributedBackend
//     handles the data-Raft propose + apply-error propagation per Phase A)
//     and then commits ObjectIndex on the meta-Raft so the cluster view is
//     consistent with the data plane.
//   - forward path: streams the body to the owner via ForwardSender.SendStream
//     using AppendObjectForwardArgs; the receiver proposes the data append and
//     commits ObjectIndex. The ingress node then re-proposes the same index
//     entry so its local meta-FSM observes read-your-writes promptly.
//
// Stale placement retry: the FSM-level ErrStalePlacement signal (see apply.go)
// is observable on the local-exec branch only because forward replies carry
// ForwardStatus enums rather than the raw sentinel. Forwarded requests are
// single-attempt for now; if rebalance races become observable we'll thread a
// new ForwardStatus value in a follow-up. The local-branch retry is bounded
// at maxAppendStaleRetries.
func (c *ClusterCoordinator) AppendObject(ctx context.Context, bucket, key string, expectedOffset int64, r io.Reader) (*storage.Object, error) {
	if err := c.requireObjectBucket(ctx, bucket); err != nil {
		return nil, err
	}
	c.ensureGenZero(ctx)
	target, group, err := c.routeAppendOrBucket(bucket, key, expectedOffset)
	if err != nil {
		return nil, err
	}
	ctx = contextWithObjectWritePlacement(ctx, group)

	// Local-exec branch — DistributedBackend.AppendObject already performs the
	// cluster-aware pre-check (offset/cap/non-appendable). We add a bounded
	// retry on ErrStalePlacement so a placement rebalance window doesn't
	// surface as a 503 to the caller.
	if gb, err := c.runtimeState().localExec.ResolveOwnerWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return c.appendObjectLocalWithRetry(ctx, gb, bucket, key, expectedOffset, r)
	}

	obj, err := c.forwardRuntime().appendObject(ctx, target, group, bucket, key, expectedOffset, r)
	if err != nil {
		return nil, err
	}
	c.waitLocalAppendVisible(ctx, target, bucket, key, obj)
	return obj, nil
}

func (c *ClusterCoordinator) waitLocalAppendVisible(ctx context.Context, target RouteTarget, bucket, key string, want *storage.Object) {
	if want == nil || !target.SelfIsVoter || c.groups == nil {
		return
	}
	gb := c.localBackend(target.GroupID)
	if gb == nil {
		return
	}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		obj, err := gb.HeadObject(waitCtx, bucket, key)
		if err == nil && obj.Size >= want.Size {
			return
		}
		select {
		case <-waitCtx.Done():
			log.Warn().
				Str("event", "append_local_visibility_wait_timeout").
				Str("bucket", bucket).
				Str("key", key).
				Str("group_id", target.GroupID).
				Int64("want_size", want.Size).
				Err(waitCtx.Err()).
				Msg("append completed before ingress local replica observed appended object")
			return
		case <-ticker.C:
		}
	}
}

// maxAppendStaleRetries bounds the transparent retry on FSM-level
// ErrStalePlacement before the coordinator surfaces it to the caller.
const maxAppendStaleRetries = 2

// appendObjectLocalWithRetry calls gb.AppendObject and retries on
// ErrStalePlacement up to maxAppendStaleRetries times. The body must be a
// Seeker so we can rewind between attempts; non-seekable readers are
// buffered once into memory under the existing c.maxBody cap.
func (c *ClusterCoordinator) appendObjectLocalWithRetry(
	ctx context.Context, gb *GroupBackend, bucket, key string, expectedOffset int64, r io.Reader,
) (*storage.Object, error) {
	seeker, ok := r.(io.Seeker)
	var buffered []byte
	if !ok {
		body, err := readBoundedBody(r, c.maxBody)
		if err != nil {
			return nil, err
		}
		buffered = body
	}

	var lastErr error
	for attempt := 0; attempt <= maxAppendStaleRetries; attempt++ {
		var body io.Reader
		if buffered != nil {
			body = bytes.NewReader(buffered)
		} else {
			if attempt > 0 {
				if _, err := seeker.Seek(0, io.SeekStart); err != nil {
					return nil, fmt.Errorf("rewind for retry: %w", err)
				}
			}
			body = r
		}
		obj, err := gb.AppendObject(ctx, bucket, key, expectedOffset, body)
		if err == nil {
			return obj, nil
		}
		if !errors.Is(err, ErrStalePlacement) {
			return nil, err
		}
		lastErr = err
	}
	return nil, fmt.Errorf("append: stale placement after %d retries: %w", maxAppendStaleRetries, lastErr)
}

func forwardBodyExceedsSingleFrameCap(r io.Reader, maxBody int64) bool {
	seeker, ok := r.(io.Seeker)
	if !ok {
		return true
	}
	cur, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return true
	}
	end, err := seeker.Seek(0, io.SeekEnd)
	if _, seekErr := seeker.Seek(cur, io.SeekStart); err == nil && seekErr != nil {
		err = seekErr
	}
	if err != nil {
		return true
	}
	return end-cur > maxBody
}

// shouldStreamForwardBody decides whether a forwarded PutObject/UploadPart body
// streams (true) or rides in a single args FlatBuffer frame (false). It streams
// when the body exceeds the maxBody single-frame cap OR is at least
// minForwardStreamBytes. Non-seekable bodies always stream (size unknown). The
// io.Seeker size probe rewinds to the original offset, so it does not consume
// the body.
func shouldStreamForwardBody(r io.Reader, maxBody int64) bool {
	if forwardBodyExceedsSingleFrameCap(r, maxBody) {
		return true
	}
	seeker, ok := r.(io.Seeker)
	if !ok {
		return true
	}
	cur, err := seeker.Seek(0, io.SeekCurrent)
	if err != nil {
		return true
	}
	end, err := seeker.Seek(0, io.SeekEnd)
	if _, seekErr := seeker.Seek(cur, io.SeekStart); err == nil && seekErr != nil {
		err = seekErr
	}
	if err != nil {
		return true
	}
	return end-cur >= minForwardStreamBytes
}
