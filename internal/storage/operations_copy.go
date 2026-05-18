package storage

import (
	"context"
	"io"
	"strings"
	"time"
)

type ObjectRef struct {
	Bucket    string
	Key       string
	VersionID string
}

type CopyMetadataDirective string

const (
	CopyMetadataCopy    CopyMetadataDirective = "COPY"
	CopyMetadataReplace CopyMetadataDirective = "REPLACE"
)

type CopyPreconditions struct {
	IfMatch           string
	IfNoneMatch       string
	IfModifiedSince   *time.Time
	IfUnmodifiedSince *time.Time
}

type CopyObjectRequest struct {
	Source            ObjectRef
	Destination       ObjectRef
	MetadataDirective CopyMetadataDirective
	ContentType       string
	UserMetadata      map[string]string
	SystemMetadata    ObjectSystemMetadata
	Preconditions     CopyPreconditions
	ACL               *uint8
}

type CopyObjectResult struct {
	Object       ObjectFacts
	Previous     PreviousObject
	SSEAlgorithm string
}

type CopyObjectAccelerationRequest struct {
	SourceRef      ObjectRef
	DestinationRef ObjectRef
	SourceObject   *Object
	ContentType    string
	UserMetadata   map[string]string
	SystemMetadata ObjectSystemMetadata
}

type copyObjectAccelerator interface {
	CopyObjectWithRequest(ctx context.Context, req CopyObjectAccelerationRequest) (*Object, error)
}

func (o *Operations) CopyObject(ctx context.Context, req CopyObjectRequest) (*CopyObjectResult, error) {
	return o.copyObject(ctx, req)
}

func (o *Operations) CopyObjectWithResult(ctx context.Context, req CopyObjectRequest) (*CopyObjectResult, error) {
	return o.CopyObject(ctx, req)
}

func (o *Operations) copyObject(ctx context.Context, req CopyObjectRequest) (*CopyObjectResult, error) {
	req = normalizeCopyObjectRequest(req)
	plan := o.planForCall()

	srcObj, err := o.headCopySource(ctx, plan, req.Source)
	if err != nil {
		return nil, err
	}
	if srcObj.IsDeleteMarker {
		if req.Source.VersionID != "" {
			return nil, InvalidCopySourceError{Reason: CopySourceIsDeleteMarker}
		}
		return nil, ErrObjectNotFound
	}
	if err := validateCopyObjectRequest(req); err != nil {
		return nil, err
	}
	if err := evaluateCopyPreconditions(srcObj, req.Preconditions); err != nil {
		return nil, err
	}
	if isSameDestinationNoop(req) {
		return nil, InvalidCopySourceError{Reason: CopySourceSameAsDestinationNoop}
	}
	previous, err := o.previousObject(ctx, req.Destination.Bucket, req.Destination.Key)
	if err != nil {
		return nil, err
	}

	contentType := srcObj.ContentType
	if req.MetadataDirective == CopyMetadataReplace {
		contentType = req.ContentType
	}
	userMetadata := copyObjectUserMetadata(req, srcObj)
	systemMetadata := copyObjectSystemMetadata(req, srcObj)

	if req.ACL == nil && len(userMetadata) == 0 && systemMetadata.empty() && canUseCopyObjectAccelerator(req) && plan.copyObjectAccelerator != nil {
		obj, err := plan.copyObjectAccelerator.CopyObjectWithRequest(ctx, CopyObjectAccelerationRequest{
			SourceRef:      req.Source,
			DestinationRef: req.Destination,
			SourceObject:   srcObj,
			ContentType:    contentType,
		})
		if err != nil {
			return nil, err
		}
		facts, err := mutationObjectFacts("CopyObject", obj)
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: facts, Previous: previous, SSEAlgorithm: facts.SSEAlgorithm}, nil
	}

	if req.ACL == nil && len(userMetadata) == 0 && systemMetadata.empty() && canUseSimpleCopier(req) && plan.copier != nil {
		obj, err := plan.copier.CopyObject(req.Source.Bucket, req.Source.Key, req.Destination.Bucket, req.Destination.Key)
		if err != nil {
			return nil, err
		}
		facts, err := mutationObjectFacts("CopyObject", obj)
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: facts, Previous: previous, SSEAlgorithm: facts.SSEAlgorithm}, nil
	}

	rc, _, err := o.openCopySource(ctx, plan, req.Source)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	obj, err := o.putObjectWithRequest(ctx, PutObjectRequest{
		Bucket:         req.Destination.Bucket,
		Key:            req.Destination.Key,
		Body:           rc,
		ContentType:    contentType,
		ACL:            req.ACL,
		UserMetadata:   userMetadata,
		SystemMetadata: systemMetadata,
	})
	if err != nil {
		return nil, err
	}
	facts, err := mutationObjectFacts("CopyObject", obj)
	if err != nil {
		return nil, err
	}
	return &CopyObjectResult{Object: facts, Previous: previous, SSEAlgorithm: facts.SSEAlgorithm}, nil
}

func normalizeCopyObjectRequest(req CopyObjectRequest) CopyObjectRequest {
	if req.MetadataDirective == "" {
		req.MetadataDirective = CopyMetadataCopy
	}
	return req
}

func copyObjectUserMetadata(req CopyObjectRequest, srcObj *Object) map[string]string {
	if req.MetadataDirective == CopyMetadataReplace {
		return cloneStringMap(req.UserMetadata)
	}
	if srcObj == nil {
		return nil
	}
	return cloneStringMap(srcObj.UserMetadata)
}

func copyObjectSystemMetadata(req CopyObjectRequest, srcObj *Object) ObjectSystemMetadata {
	if req.MetadataDirective == CopyMetadataReplace {
		return req.SystemMetadata
	}
	if srcObj == nil {
		return ObjectSystemMetadata{}
	}
	return ObjectSystemMetadata{SSEAlgorithm: srcObj.SSEAlgorithm}
}

func (o *Operations) headCopySource(ctx context.Context, plan operationsPlan, ref ObjectRef) (*Object, error) {
	if ref.VersionID != "" {
		if plan.versionedHeader == nil {
			return nil, UnsupportedOperationError{Op: "HeadObjectVersion", Reason: UnsupportedReasonNoAdapter}
		}
		return plan.versionedHeader.HeadObjectVersion(ref.Bucket, ref.Key, ref.VersionID)
	}
	return o.backend.HeadObject(ctx, ref.Bucket, ref.Key)
}

func (o *Operations) openCopySource(ctx context.Context, plan operationsPlan, ref ObjectRef) (io.ReadCloser, *Object, error) {
	if ref.VersionID != "" {
		if plan.versionedGetter == nil {
			return nil, nil, UnsupportedOperationError{Op: "GetObjectVersion", Reason: UnsupportedReasonNoAdapter}
		}
		return plan.versionedGetter.GetObjectVersion(ref.Bucket, ref.Key, ref.VersionID)
	}
	return o.backend.GetObject(ctx, ref.Bucket, ref.Key)
}

func validateCopyObjectRequest(req CopyObjectRequest) error {
	switch req.MetadataDirective {
	case CopyMetadataCopy, CopyMetadataReplace:
		return nil
	default:
		return UnsupportedOperationError{Op: "CopyObject", Reason: UnsupportedReasonMetadataUnsupported}
	}
}

func evaluateCopyPreconditions(src *Object, cond CopyPreconditions) error {
	etag := normalizeCopyETag(src.ETag)
	if cond.IfMatch != "" {
		want, ok := normalizeCopyConditionETag(cond.IfMatch)
		if !ok {
			return InvalidCopySourceError{Reason: CopySourceUnsupportedETagSelector}
		}
		if etag != want {
			return PreconditionFailedError{Op: "CopyObject", Condition: CopyConditionIfMatch}
		}
	}
	if cond.IfUnmodifiedSince != nil {
		if time.Unix(src.LastModified, 0).After(cond.IfUnmodifiedSince.UTC().Truncate(time.Second)) {
			return PreconditionFailedError{Op: "CopyObject", Condition: CopyConditionIfUnmodifiedSince}
		}
	}
	if cond.IfNoneMatch != "" {
		want, ok := normalizeCopyConditionETag(cond.IfNoneMatch)
		if !ok {
			return InvalidCopySourceError{Reason: CopySourceUnsupportedETagSelector}
		}
		if etag == want {
			return PreconditionFailedError{Op: "CopyObject", Condition: CopyConditionIfNoneMatch}
		}
	}
	if cond.IfModifiedSince != nil {
		if !time.Unix(src.LastModified, 0).After(cond.IfModifiedSince.UTC().Truncate(time.Second)) {
			return PreconditionFailedError{Op: "CopyObject", Condition: CopyConditionIfModifiedSince}
		}
	}
	return nil
}

func normalizeCopyConditionETag(v string) (string, bool) {
	v = strings.TrimSpace(v)
	if strings.Contains(v, ",") || strings.HasPrefix(v, "W/") {
		return "", false
	}
	return normalizeCopyETag(v), true
}

func normalizeCopyETag(v string) string {
	v = strings.TrimSpace(v)
	v = strings.TrimPrefix(v, `"`)
	v = strings.TrimSuffix(v, `"`)
	return v
}

func isSameDestinationNoop(req CopyObjectRequest) bool {
	return req.Source.VersionID == "" &&
		req.Source.Bucket == req.Destination.Bucket &&
		req.Source.Key == req.Destination.Key &&
		req.MetadataDirective == CopyMetadataCopy &&
		req.ContentType == "" &&
		req.ACL == nil &&
		len(req.UserMetadata) == 0
}

func canUseSimpleCopier(req CopyObjectRequest) bool {
	return req.Source.VersionID == "" &&
		req.MetadataDirective == CopyMetadataCopy &&
		req.ContentType == "" &&
		req.ACL == nil &&
		len(req.UserMetadata) == 0 &&
		req.Preconditions.IfMatch == "" &&
		req.Preconditions.IfNoneMatch == "" &&
		req.Preconditions.IfModifiedSince == nil &&
		req.Preconditions.IfUnmodifiedSince == nil
}

func canUseCopyObjectAccelerator(req CopyObjectRequest) bool {
	return req.Source.VersionID == "" &&
		req.ACL == nil &&
		len(req.UserMetadata) == 0
}
