package storage

import "context"

type CopyObjectRequest struct {
	SourceBucket      string
	SourceKey         string
	DestinationBucket string
	DestinationKey    string
	ACL               *uint8
}

type CopyObjectResult struct {
	Object *Object
}

func (o *Operations) CopyObject(ctx context.Context, req CopyObjectRequest) (*CopyObjectResult, error) {
	if req.ACL == nil && o.plan.copier != nil {
		obj, err := o.plan.copier.CopyObject(req.SourceBucket, req.SourceKey, req.DestinationBucket, req.DestinationKey)
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: obj}, nil
	}

	rc, srcObj, err := o.backend.GetObject(ctx, req.SourceBucket, req.SourceKey)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	contentType := ""
	if srcObj != nil {
		contentType = srcObj.ContentType
	}
	if req.ACL != nil {
		obj, err := o.PutObjectWithACL(ctx, req.DestinationBucket, req.DestinationKey, rc, contentType, *req.ACL)
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: obj}, nil
	}

	obj, err := o.backend.PutObject(ctx, req.DestinationBucket, req.DestinationKey, rc, contentType)
	if err != nil {
		return nil, err
	}
	return &CopyObjectResult{Object: obj}, nil
}
