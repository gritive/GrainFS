package iceberg

import (
	"github.com/cloudwego/hertz/pkg/app/server"

	"github.com/gritive/GrainFS/internal/iam/oauth"
)

const routePathOAuthTokenSuffix = "v1/oauth/tokens"

// Register wires the iceberg REST-catalog routes onto the Hertz engine under
// each supplied prefix. Every prefix gets the full route set plus the OAuth
// token endpoint.
func (h *Handler) Register(hz *server.Hertz, prefixes ...string) {
	// Build the OAuth handler once all options have been applied.
	// IAMStore and PolicyAuthorizer may be nil in tests that don't wire them.
	if h.oauthHandler == nil && h.deps.JWTKeys != nil && h.deps.IAMStore != nil && h.deps.PolicyAuthorizer != nil {
		var resolver oauth.SAResolver = oauth.NewStoreResolver(h.deps.IAMStore)
		authorizer := oauth.Authorizer(h.deps.PolicyAuthorizer)
		if h.deps.AuditInternalAccessKey != "" && h.deps.AuditInternalSecretKey != "" {
			resolver = auditInternalOAuthResolver{
				base:      resolver,
				accessKey: h.deps.AuditInternalAccessKey,
				secretKey: h.deps.AuditInternalSecretKey,
			}
			authorizer = auditInternalOAuthAuthorizer{base: authorizer}
		}
		h.oauthHandler = newIcebergOAuthHandler(
			resolver,
			h.deps.JWTKeys,
			authorizer,
			h.deps.ClientIP,
			h.deps.NewRespWriter,
		)
	}

	// OAuth2 token endpoint — SigV4-free (carries own credentials in body).
	oauthHandle := h.oauthHandlerFunc()
	for _, prefix := range prefixes {
		hz.POST(prefix+routePathOAuthTokenSuffix, oauthHandle)
	}

	for _, prefix := range prefixes {
		h.registerIcebergAPIAt(hz, prefix)
	}
}

func (h *Handler) registerIcebergAPIAt(hz *server.Hertz, prefix string) {
	hz.GET(prefix+"v1/config", h.icebergAccessLog(h.icebergGuarded("iceberg:GetCatalogConfig", h.icebergConfig)))
	hz.POST(prefix+"v1/warehouses", h.icebergAccessLog(h.icebergGuarded("iceberg:CreateWarehouse", h.icebergEnsureWarehouse)))
	hz.DELETE(prefix+"v1/warehouses/:warehouse", h.icebergAccessLog(h.icebergGuarded("iceberg:DropWarehouse", h.icebergDeleteWarehouse)))
	hz.GET(prefix+"v1/namespaces", h.icebergAccessLog(h.icebergGuarded("iceberg:ListNamespaces", h.icebergListNamespaces)))
	hz.POST(prefix+"v1/namespaces", h.icebergAccessLog(h.icebergGuarded("iceberg:CreateNamespace", h.icebergCreateNamespace)))
	hz.GET(prefix+"v1/namespaces/:namespace", h.icebergAccessLog(h.icebergGuarded("iceberg:LoadNamespace", h.icebergLoadNamespace)))
	hz.HEAD(prefix+"v1/namespaces/:namespace", h.icebergAccessLog(h.icebergGuarded("iceberg:LoadNamespace", h.icebergHeadNamespace)))
	hz.DELETE(prefix+"v1/namespaces/:namespace", h.icebergAccessLog(h.icebergGuarded("iceberg:DropNamespace", h.icebergDeleteNamespace)))
	hz.GET(prefix+"v1/namespaces/:namespace/tables", h.icebergAccessLog(h.icebergGuarded("iceberg:ListTables", h.icebergListTables)))
	hz.POST(prefix+"v1/namespaces/:namespace/tables", h.icebergAccessLog(h.icebergGuarded("iceberg:CreateTable", h.icebergCreateTable)))
	hz.GET(prefix+"v1/namespaces/:namespace/tables/:table", h.icebergAccessLog(h.icebergGuarded("iceberg:LoadTable", h.icebergLoadTable)))
	hz.HEAD(prefix+"v1/namespaces/:namespace/tables/:table", h.icebergAccessLog(h.icebergGuarded("iceberg:LoadTable", h.icebergHeadTable)))
	hz.POST(prefix+"v1/namespaces/:namespace/tables/:table", h.icebergAccessLog(h.icebergGuarded("iceberg:CommitTable", h.icebergCommitTable)))
	hz.DELETE(prefix+"v1/namespaces/:namespace/tables/:table", h.icebergAccessLog(h.icebergGuarded("iceberg:DropTable", h.icebergDeleteTable)))
	hz.POST(prefix+"v1/transactions/commit", h.icebergAccessLog(h.icebergGuarded("iceberg:CommitTransaction", h.icebergCommitTransaction)))
	hz.Any(prefix+"*path", h.icebergAccessLog(h.icebergGuarded("iceberg:Unsupported", h.icebergUnsupported)))
}
