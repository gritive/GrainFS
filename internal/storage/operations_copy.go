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

type CopySourceConditions struct {
	IfMatch           string
	IfNoneMatch       string
	IfModifiedSince   *time.Time
	IfUnmodifiedSince *time.Time
}

type CopyObjectRequest struct {
	SourceBucket      string
	SourceKey         string
	DestinationBucket string
	DestinationKey    string
	ACL               *uint8 // legacy fields above are normalized into Source/Destination.

	Source            ObjectRef
	Destination       ObjectRef
	MetadataDirective CopyMetadataDirective
	ContentType       string
	UserMetadata      map[string]string
	Conditions        CopySourceConditions
}

type CopyObjectResult struct {
	Object *Object
}

type CopyObjectAdapterRequest struct {
	SourceRef      ObjectRef
	DestinationRef ObjectRef
	SourceObject   *Object
	ContentType    string
}

type CopyObjectAdapter interface {
	CopyObjectWithRequest(ctx context.Context, req CopyObjectAdapterRequest) (*Object, error)
}

func (o *Operations) CopyObject(ctx context.Context, req CopyObjectRequest) (*CopyObjectResult, error) {
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
	if err := evaluateCopySourceConditions(srcObj, req.Conditions); err != nil {
		return nil, err
	}
	if isSameDestinationNoop(req) {
		return nil, InvalidCopySourceError{Reason: CopySourceSameAsDestinationNoop}
	}

	contentType := srcObj.ContentType
	if req.MetadataDirective == CopyMetadataReplace {
		contentType = req.ContentType
	}

	if req.ACL == nil && canUseCopyObjectAdapter(req) && plan.copyObjectAdapter != nil {
		obj, err := plan.copyObjectAdapter.CopyObjectWithRequest(ctx, CopyObjectAdapterRequest{
			SourceRef:      req.Source,
			DestinationRef: req.Destination,
			SourceObject:   srcObj,
			ContentType:    contentType,
		})
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: obj}, nil
	}

	if req.ACL == nil && canUseSimpleCopier(req) && plan.copier != nil {
		obj, err := plan.copier.CopyObject(req.Source.Bucket, req.Source.Key, req.Destination.Bucket, req.Destination.Key)
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: obj}, nil
	}

	rc, _, err := o.openCopySource(ctx, plan, req.Source)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

	if req.ACL != nil {
		obj, err := o.PutObjectWithACL(ctx, req.Destination.Bucket, req.Destination.Key, rc, contentType, *req.ACL)
		if err != nil {
			return nil, err
		}
		return &CopyObjectResult{Object: obj}, nil
	}

	obj, err := o.backend.PutObject(ctx, req.Destination.Bucket, req.Destination.Key, rc, contentType)
	if err != nil {
		return nil, err
	}
	return &CopyObjectResult{Object: obj}, nil
}

func normalizeCopyObjectRequest(req CopyObjectRequest) CopyObjectRequest {
	if req.Source.Bucket == "" && req.Source.Key == "" {
		req.Source = ObjectRef{Bucket: req.SourceBucket, Key: req.SourceKey}
	}
	if req.Destination.Bucket == "" && req.Destination.Key == "" {
		req.Destination = ObjectRef{Bucket: req.DestinationBucket, Key: req.DestinationKey}
	}
	if req.MetadataDirective == "" {
		req.MetadataDirective = CopyMetadataCopy
	}
	return req
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
	if len(req.UserMetadata) > 0 {
		return UnsupportedOperationError{Op: "CopyObject", Reason: UnsupportedReasonMetadataUnsupported}
	}
	switch req.MetadataDirective {
	case CopyMetadataCopy, CopyMetadataReplace:
		return nil
	default:
		return UnsupportedOperationError{Op: "CopyObject", Reason: UnsupportedReasonMetadataUnsupported}
	}
}

func evaluateCopySourceConditions(src *Object, cond CopySourceConditions) error {
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
		req.Conditions.IfMatch == "" &&
		req.Conditions.IfNoneMatch == "" &&
		req.Conditions.IfModifiedSince == nil &&
		req.Conditions.IfUnmodifiedSince == nil
}

func canUseCopyObjectAdapter(req CopyObjectRequest) bool {
	return req.Source.VersionID == "" &&
		req.ACL == nil &&
		len(req.UserMetadata) == 0
}
