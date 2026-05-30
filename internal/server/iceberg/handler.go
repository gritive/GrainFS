package iceberg

import (
	"context"
	"net/http"
	"sync/atomic"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/gritive/GrainFS/internal/audit"
	"github.com/gritive/GrainFS/internal/iam"
	iamjwt "github.com/gritive/GrainFS/internal/iam/jwt"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/icebergcatalog"
	"github.com/gritive/GrainFS/internal/storage"
)

// PolicyAuthorizer mirrors the core seam (interface defined at point of use).
type PolicyAuthorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}

// Deps carries everything the iceberg handlers need from core, as concrete
// pointers (cheap to share) + closures (so iceberg never imports server).
type Deps struct {
	Ops              *storage.Operations
	IAMStore         *iam.Store
	PolicyAuthorizer PolicyAuthorizer
	JWTKeys          *iamjwt.KeySet
	Catalog          icebergcatalog.Catalog

	AuditInternalAccessKey string
	AuditInternalSecretKey string
	AuditNodeID            string

	ClientIP            func(*app.RequestContext) string
	MutationDisabled    func(c *app.RequestContext, operation string) bool
	FeatureAvailable    func() bool
	AppendAuditEvent    func(ctx context.Context, ev audit.S3Event) // normalizes internally
	AuditSinkConfigured func() bool
	RequestID           func(ctx context.Context) string
	AccessKey           func(ctx context.Context) string
	NewRespWriter       func(*app.RequestContext) http.ResponseWriter
}

type Handler struct {
	deps Deps

	catalog      icebergcatalog.Catalog
	oauthHandler *icebergOAuthHandler // type lives in this package (moved)

	accessLogEnabled      atomic.Bool
	commitSlowThresholdNs atomic.Int64
}

func NewHandler(d Deps) *Handler {
	return &Handler{deps: d, catalog: d.Catalog}
}

// ClaimsKey is the context key under which core stores iceberg JWT claims and
// iceberg handlers read them. Lives here so identity is shared via the one-way
// core→iceberg import edge.
type claimsKeyType struct{}

var ClaimsKey = claimsKeyType{}
