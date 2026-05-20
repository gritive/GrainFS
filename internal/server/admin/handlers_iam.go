package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/iam"
)

// IAM admin handlers — volume pattern pass-throughs.
// Each function delegates to d.IAM (IAMService interface), keeping transport
// logic in hertz_adapter.go and business logic in internal/iam/admin_api.go.

func CreateSA(ctx context.Context, d *Deps, req iam.SACreateRequest) (iam.SACreateResponse, error) {
	return d.IAM.CreateSA(ctx, req)
}

func ListSA(ctx context.Context, d *Deps) ([]iam.SAListItem, error) {
	return d.IAM.ListSA(ctx)
}

func GetSA(ctx context.Context, d *Deps, saID string) (iam.SAGetResponse, error) {
	return d.IAM.GetSA(ctx, saID)
}

func DeleteSA(ctx context.Context, d *Deps, saID string) error {
	return d.IAM.DeleteSA(ctx, saID)
}

func PutGrant(ctx context.Context, d *Deps, req iam.GrantPutRequest) error {
	return d.IAM.PutGrant(ctx, req)
}

func DeleteGrant(ctx context.Context, d *Deps, req iam.GrantDeleteRequest) error {
	return d.IAM.DeleteGrant(ctx, req)
}

func CreateKey(ctx context.Context, d *Deps, saID string, req iam.KeyCreateRequest) (iam.KeyCreateResponse, error) {
	return d.IAM.CreateKey(ctx, saID, req)
}

func RevokeKey(ctx context.Context, d *Deps, saID, accessKey string) error {
	return d.IAM.RevokeKey(ctx, saID, accessKey)
}

func PutBucketUpstream(ctx context.Context, d *Deps, req iam.BucketUpstreamPutRequest) error {
	return d.IAM.PutBucketUpstream(ctx, req)
}

func GetBucketUpstream(ctx context.Context, d *Deps, bucket string) (iam.BucketUpstreamItem, error) {
	return d.IAM.GetBucketUpstream(ctx, bucket)
}

func ListBucketUpstreams(ctx context.Context, d *Deps) ([]iam.BucketUpstreamItem, error) {
	return d.IAM.ListBucketUpstreams(ctx)
}

func DeleteBucketUpstream(ctx context.Context, d *Deps, bucket string) error {
	return d.IAM.DeleteBucketUpstream(ctx, bucket)
}

func CutoverBucketUpstream(ctx context.Context, d *Deps, bucket string) error {
	return d.IAM.CutoverBucketUpstream(ctx, bucket)
}
