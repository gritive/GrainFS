package cluster

import (
	"context"
	"io"

	"github.com/gritive/GrainFS/internal/compat"
	"github.com/gritive/GrainFS/internal/storage"
)

type multipartRuntime struct {
	c *ClusterCoordinator
}

func (c *ClusterCoordinator) multipartRuntime() multipartRuntime {
	return multipartRuntime{c: c}
}

// createMultipartUpload group-encodes the returned uploadID with the routed
// placement group (see multipart_upload_id.go) so session ops issued on OTHER
// nodes — whose boot-frozen hash candidate sets may diverge — route back to
// the owning group. Backends only ever see the raw ID.
func (r multipartRuntime) createMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	ctx, target, _, err := r.prepareCreate(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	var upload *storage.MultipartUpload
	if gb, err := r.c.runtimeState().localExec.ResolveOwnerWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		upload, err = gb.CreateMultipartUpload(ctx, bucket, key, contentType)
		if err != nil {
			return nil, err
		}
	} else {
		upload, err = r.c.forwardRuntime().createMultipartUpload(ctx, target, bucket, key, contentType, nil)
		if err != nil {
			return nil, err
		}
	}
	return r.c.wrapMultipartUpload(upload, target.GroupID), nil
}

func (r multipartRuntime) createMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error) {
	ctx, target, _, err := r.prepareCreate(ctx, bucket, key)
	if err != nil {
		return "", err
	}
	if gb, err := r.c.runtimeState().localExec.ResolveOwnerWrite(ctx, target); err != nil {
		return "", err
	} else if gb != nil {
		uploadID, err := gb.CreateMultipartUploadWithTags(ctx, bucket, key, contentType, tags)
		if err != nil {
			return "", err
		}
		return r.c.wrapMultipartUploadID(uploadID, target.GroupID), nil
	}
	upload, err := r.c.forwardRuntime().createMultipartUpload(ctx, target, bucket, key, contentType, tags)
	if err != nil {
		return "", err
	}
	return r.c.wrapMultipartUploadID(upload.UploadID, target.GroupID), nil
}

func (r multipartRuntime) completeMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	ctx, target, group, rawID, err := r.prepareSession(ctx, bucket, key, uploadID)
	if err != nil {
		return nil, err
	}
	ctx = contextWithObjectWritePlacement(ctx, group)
	if gb, err := r.c.runtimeState().localExec.ResolveOwnerWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.CompleteMultipartUpload(ctx, bucket, key, rawID, parts)
	}
	return r.c.forwardRuntime().completeMultipartUpload(ctx, target, bucket, key, rawID, parts)
}

func (r multipartRuntime) uploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader, contentMD5Hex string) (*storage.Part, error) {
	ctx, target, _, rawID, err := r.prepareSession(ctx, bucket, key, uploadID)
	if err != nil {
		return nil, err
	}
	ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	if gb, err := r.c.runtimeState().localExec.ResolveOwnerWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.UploadPart(ctx, bucket, key, rawID, partNumber, body, contentMD5Hex)
	}
	return r.c.forwardRuntime().uploadPart(ctx, target, bucket, key, rawID, partNumber, body, contentMD5Hex)
}

func (r multipartRuntime) abortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	ctx, target, _, rawID, err := r.prepareSession(ctx, bucket, key, uploadID)
	if err != nil {
		return err
	}
	ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	if gb, err := r.c.runtimeState().localExec.ResolveOwnerWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.AbortMultipartUpload(ctx, bucket, key, rawID)
	}
	return r.c.forwardRuntime().abortMultipartUpload(ctx, target, bucket, key, rawID)
}

func (r multipartRuntime) prepareCreate(ctx context.Context, bucket, key string) (context.Context, RouteTarget, ShardGroupEntry, error) {
	ctx, target, group, err := r.prepareMutation(ctx, bucket, key)
	if err != nil {
		return ctx, RouteTarget{}, ShardGroupEntry{}, err
	}
	ctx = contextWithObjectWritePlacement(ctx, group)
	if err := r.c.requireMultipartListingPeerCapability(compat.OperationCreateMultipartUpload, r.c.multipartListingCapabilityPeers(target, group)); err != nil {
		return ctx, RouteTarget{}, ShardGroupEntry{}, err
	}
	return ctx, target, group, nil
}

func (r multipartRuntime) prepareMutation(ctx context.Context, bucket, key string) (context.Context, RouteTarget, ShardGroupEntry, error) {
	if err := r.c.requireObjectBucket(ctx, bucket); err != nil {
		return ctx, RouteTarget{}, ShardGroupEntry{}, err
	}
	target, group, err := r.c.routeOwnerWriteOrBucket(bucket, key)
	if err != nil {
		return ctx, RouteTarget{}, ShardGroupEntry{}, err
	}
	return ctx, target, group, nil
}

// prepareSession resolves routing for an op on an EXISTING multipart session
// (routeMultipartSession: group-encoded uploadID → direct group route,
// un-prefixed → legacy hash route) and returns the raw backend uploadID.
func (r multipartRuntime) prepareSession(ctx context.Context, bucket, key, uploadID string) (context.Context, RouteTarget, ShardGroupEntry, string, error) {
	if err := r.c.requireObjectBucket(ctx, bucket); err != nil {
		return ctx, RouteTarget{}, ShardGroupEntry{}, "", err
	}
	target, group, rawID, err := r.c.routeMultipartSession(bucket, key, uploadID)
	if err != nil {
		return ctx, RouteTarget{}, ShardGroupEntry{}, "", err
	}
	return ctx, target, group, rawID, nil
}
