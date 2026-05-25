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

func (r multipartRuntime) createMultipartUpload(ctx context.Context, bucket, key, contentType string) (*storage.MultipartUpload, error) {
	ctx, target, _, err := r.prepareCreate(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	if gb, err := r.c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.CreateMultipartUpload(ctx, bucket, key, contentType)
	}
	return r.c.forwardRuntime().createMultipartUpload(ctx, target, bucket, key, contentType, nil)
}

func (r multipartRuntime) createMultipartUploadWithTags(ctx context.Context, bucket, key, contentType string, tags []storage.Tag) (string, error) {
	ctx, target, _, err := r.prepareCreate(ctx, bucket, key)
	if err != nil {
		return "", err
	}
	if gb, err := r.c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return "", err
	} else if gb != nil {
		return gb.CreateMultipartUploadWithTags(ctx, bucket, key, contentType, tags)
	}
	upload, err := r.c.forwardRuntime().createMultipartUpload(ctx, target, bucket, key, contentType, tags)
	if err != nil {
		return "", err
	}
	return upload.UploadID, nil
}

func (r multipartRuntime) completeMultipartUpload(ctx context.Context, bucket, key, uploadID string, parts []storage.Part) (*storage.Object, error) {
	ctx, target, group, err := r.prepareMutation(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	if r.c.indexWriter != nil {
		ctx = contextWithObjectWritePlacement(ctx, group)
	}
	if gb, err := r.c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		obj, err := gb.CompleteMultipartUpload(ctx, bucket, key, uploadID, parts)
		if err != nil {
			return nil, err
		}
		return obj, r.c.commitObjectIndex(ctx, bucket, key, obj, group, false)
	}
	obj, err := r.c.forwardRuntime().completeMultipartUpload(ctx, target, bucket, key, uploadID, parts)
	if err != nil {
		return nil, err
	}
	return obj, r.c.commitObjectIndex(ctx, bucket, key, obj, group, false)
}

func (r multipartRuntime) uploadPart(ctx context.Context, bucket, key, uploadID string, partNumber int, body io.Reader) (*storage.Part, error) {
	ctx, target, _, err := r.prepareMutation(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	if r.c.indexWriter != nil {
		ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	}
	if gb, err := r.c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return nil, err
	} else if gb != nil {
		return gb.UploadPart(ctx, bucket, key, uploadID, partNumber, body)
	}
	return r.c.forwardRuntime().uploadPart(ctx, target, bucket, key, uploadID, partNumber, body)
}

func (r multipartRuntime) abortMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	ctx, target, _, err := r.prepareMutation(ctx, bucket, key)
	if err != nil {
		return err
	}
	if r.c.indexWriter != nil {
		ctx = ContextWithPlacementGroup(ctx, target.GroupID)
	}
	if gb, err := r.c.runtimeState().localExec.ResolveWrite(ctx, target); err != nil {
		return err
	} else if gb != nil {
		return gb.AbortMultipartUpload(ctx, bucket, key, uploadID)
	}
	return r.c.forwardRuntime().abortMultipartUpload(ctx, target, bucket, key, uploadID)
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
	target, group, err := r.c.routeWriteOrBucket(bucket, key)
	if err != nil {
		return ctx, RouteTarget{}, ShardGroupEntry{}, err
	}
	return ctx, target, group, nil
}
