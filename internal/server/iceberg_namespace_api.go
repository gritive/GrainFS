package server

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/protocol/consts"

	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
)

func (s *Server) requireIceberg(c *app.RequestContext) (icebergcatalog.Catalog, bool) {
	if !s.routeFeatureAvailable(routeFeatureIceberg) {
		writeIcebergError(c, consts.StatusNotImplemented, "NotImplementedException", "Iceberg REST Catalog is not available: server started without --audit-iceberg or catalog initialization failed")
		return nil, false
	}
	return s.icebergCatalogStore(), true
}

func (s *Server) icebergConfig(_ context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	overrides := s.icebergS3CredOverrides(store.Warehouse())
	if len(overrides) > 0 {
		// Publish the same host:port the client just connected to as
		// s3.endpoint. iceberg-go's REST catalog reads this to build
		// its data-plane S3 client; without it the client falls back
		// to the AWS public endpoint and our local parquet reads
		// return 403 InvalidAccessKeyId.
		if host := string(c.Request.Host()); host != "" {
			overrides["s3.endpoint"] = "http://" + host
		}
	}
	c.JSON(consts.StatusOK, map[string]map[string]string{
		"defaults":  {"warehouse": store.Warehouse()},
		"overrides": overrides,
	})
}

// icebergS3CredOverrides picks one IAM ServiceAccount that already has at
// least write-role access to the warehouse bucket (per the same per-bucket
// grants that gate S3 SigV4 requests) and publishes its active
// access_key / secret_key in the Iceberg REST `/v1/config` "overrides"
// map. iceberg-go's REST client uses these to build the S3 client that
// reads parquet footers during transaction.AddFiles — without them the
// client falls back to the ambient AWS credential chain and our bench
// gets `403 InvalidAccessKeyId` long before any CommitTable hits this
// server (observed in warp iceberg `sustained`: 1832/1870 ops failed,
// 0 commits reached the server).
//
// The bucket is parsed from the catalog's warehouse location (e.g.
// `s3://grainfs-tables/warehouse` → `grainfs-tables`). The SA selection
// honours the existing IAM model: whichever account already holds the
// authority to write into that bucket is the right principal to also
// expose for data-plane access. No separate "iceberg data plane SA"
// concept is needed — and no env var or CLI flag — because IAM already
// expresses the trust relationship.
//
// Returns an empty map when:
//   - the IAM store is missing,
//   - the catalog warehouse location does not parse as `s3://bucket/...`,
//   - no SA holds RoleWrite or higher on the bucket (including
//     wildcards), or
//   - the qualifying SA has no active access key.
//
// All lookups are read-only against the IAM store snapshot — no secrets
// touch disk or env state in this path. Production deployments that want
// to prevent credential publication can revoke the relevant grants or
// gate this endpoint behind authn (a separate follow-up).
func (s *Server) icebergS3CredOverrides(warehouse string) map[string]string {
	if s.iamStore == nil {
		return map[string]string{}
	}
	bucket := bucketFromS3Location(warehouse)
	if bucket == "" {
		return map[string]string{}
	}
	key, ok := s.iamStore.FirstActiveKeyForBucketGrant(bucket, iam.RoleWrite)
	if !ok || key == nil || key.SecretKey == "" {
		return map[string]string{}
	}
	return map[string]string{
		"s3.access-key-id":     key.AccessKey,
		"s3.secret-access-key": key.SecretKey,
		// path-style required: GrainFS does not implement virtual-host
		// bucket addressing. iceberg-go's AddFiles passes the table's
		// metadata-location as an `s3://bucket/key` URL and the client
		// builds the GET URL from that; without path-style the client
		// would try `https://bucket.endpoint/key` which we don't route.
		"s3.path-style-access": "true",
	}
}

// bucketFromS3Location extracts the bucket from an `s3://bucket/prefix`
// warehouse string. Returns empty when the input isn't a well-formed
// s3:// URL (e.g., a local file path or empty value), which the caller
// treats as "no credentials to publish".
func bucketFromS3Location(loc string) string {
	const prefix = "s3://"
	if len(loc) < len(prefix) || loc[:len(prefix)] != prefix {
		return ""
	}
	rest := loc[len(prefix):]
	for i := 0; i < len(rest); i++ {
		if rest[i] == '/' {
			return rest[:i]
		}
	}
	return rest
}

func (s *Server) icebergEnsureWarehouse(_ context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	c.JSON(consts.StatusOK, map[string]string{
		"name":      string(c.QueryArgs().Peek("name")),
		"warehouse": store.Warehouse(),
	})
}

func (s *Server) icebergDeleteWarehouse(_ context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	if _, ok := s.requireIceberg(c); !ok {
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergListNamespaces(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	namespaces, err := store.ListNamespaces(ctx)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespaces": namespaces})
}

func (s *Server) icebergCreateNamespace(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	req, err := parseIcebergCreateNamespaceRequest(c.Request.Body())
	if err != nil {
		writeIcebergError(c, consts.StatusBadRequest, "BadRequestException", "invalid namespace request")
		return
	}
	if err := store.CreateNamespace(ctx, req.Namespace, req.Properties); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespace": req.Namespace, "properties": nonNilMap(req.Properties)})
}

func (s *Server) icebergLoadNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	ns := []string{c.Param("namespace")}
	props, err := store.LoadNamespace(ctx, ns)
	if err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.JSON(consts.StatusOK, map[string]any{"namespace": ns, "properties": props})
}

func (s *Server) icebergHeadNamespace(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if _, err := store.LoadNamespace(ctx, []string{c.Param("namespace")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergDeleteNamespace(ctx context.Context, c *app.RequestContext) {
	if s.blockIfMutationDisabled(c, "iceberg_catalog_mutation") {
		return
	}
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	if err := store.DeleteNamespace(ctx, []string{c.Param("namespace")}); err != nil {
		writeIcebergMappedError(c, err)
		return
	}
	c.Status(consts.StatusNoContent)
}

func (s *Server) icebergUnsupported(_ context.Context, c *app.RequestContext) {
	writeIcebergError(c, consts.StatusNotImplemented, "NotImplementedException", "unsupported Iceberg REST Catalog operation")
}
