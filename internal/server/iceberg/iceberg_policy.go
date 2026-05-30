package iceberg

import "github.com/gritive/GrainFS/internal/iam/policy"

// policyIcebergConfigContext returns the policy.RequestContext for an
// iceberg:GetCatalogConfig check against a warehouse bucket. Used by
// icebergS3CredOverrides to gate credential forwarding.
func policyIcebergConfigContext(bucket string) policy.RequestContext {
	return policy.RequestContext{
		Action:   "iceberg:GetCatalogConfig",
		Resource: "arn:aws:s3:::" + bucket,
	}
}
