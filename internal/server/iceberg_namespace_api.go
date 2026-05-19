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

func (s *Server) icebergConfig(ctx context.Context, c *app.RequestContext) {
	store, ok := s.requireIceberg(c)
	if !ok {
		return
	}
	overrides := s.icebergS3CredOverrides(ctx, store.Warehouse())
	if len(overrides) > 0 {
		// Publish the same host:port the client just connected to as
		// s3.endpoint. iceberg-go's REST catalog reads this to build
		// its data-plane S3 client; without it the client falls back
		// to the AWS public endpoint and our local parquet reads
		// return 403 InvalidAccessKeyId. Mirror the scheme the client
		// used so an HTTPS caller doesn't get downgraded to plaintext.
		if host := string(c.Request.Host()); host != "" {
			scheme := string(c.Request.Scheme())
			if scheme == "" {
				scheme = "http"
			}
			overrides["s3.endpoint"] = scheme + "://" + host
		}
	}
	c.JSON(consts.StatusOK, map[string]map[string]string{
		"defaults":  {"warehouse": store.Warehouse()},
		"overrides": overrides,
	})
}

// icebergS3CredOverrides publishes the *caller's own* IAM access_key /
// secret_key in the Iceberg REST `/v1/config` "overrides" map so that
// iceberg-go's REST client can build a data-plane S3 client that reads
// parquet footers during transaction.AddFiles. Without these the client
// falls back to the ambient AWS credential chain and gets `403
// InvalidAccessKeyId` long before any CommitTable hits this server
// (observed in warp iceberg `sustained`: 1832/1870 ops failed, 0 commits
// reached the server).
//
// The bucket is parsed from the catalog's warehouse location (e.g.
// `s3://grainfs-tables/warehouse` → `grainfs-tables`). The caller's
// access_key is taken from ctx (set by the authn middleware after SigV4
// verification) and looked up to retrieve the matching secret_key. The
// caller must hold at least RoleRead on the warehouse bucket — otherwise
// we publish nothing rather than handing out useful creds to a principal
// that wouldn't be able to use them anyway.
//
// This deliberately does NOT pick "some other SA that has the right grant
// already" — that would amplify a RoleRead caller into RoleAdmin-class
// data-plane credentials, which is a privilege escalation. The caller
// gets the same authority on the data plane that they already authn'd
// for on the catalog plane.
//
// Returns an empty map when:
//   - the IAM store is missing,
//   - the catalog warehouse location does not parse as `s3://bucket/...`,
//   - the request has no resolved caller access key (e.g., the authn
//     middleware was skipped — should not happen on the SigV4-gated
//     iceberg routes),
//   - the caller's access key is unknown, revoked, or expired,
//   - the caller holds less than RoleRead on the warehouse bucket
//     (including via wildcard).
//
// All lookups are read-only against the IAM store snapshot — no secrets
// touch disk or env state in this path.
func (s *Server) icebergS3CredOverrides(ctx context.Context, warehouse string) map[string]string {
	if s.iamStore == nil {
		return map[string]string{}
	}
	bucket := bucketFromS3Location(warehouse)
	if bucket == "" {
		return map[string]string{}
	}
	accessKey := AccessKeyFromContext(ctx)
	if accessKey == "" {
		return map[string]string{}
	}
	key, ok := s.iamStore.LookupKey(accessKey)
	if !ok || key == nil || key.SecretKey == "" {
		return map[string]string{}
	}
	if s.iamStore.LookupGrant(key.SAID, bucket) < iam.RoleRead {
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
