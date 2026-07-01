// Package admin provides transport-agnostic handlers for GrainFS administrative
// operations (volume lifecycle, dashboard token issuance). The same handler
// functions are wired into both the Unix-socket admin server (used by the
// `grainfs` CLI) and the data-plane `/ui/api/*` routes (used by the web UI).
package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/incident"
)

// Deps bundles the shared dependencies required by every admin handler.
// Caller is responsible for constructing this struct at process startup.
type Deps struct {
	Incident             incident.StateStore       // List(ctx, limit) — optional, nil OK
	Director             DirectorAPI               // optional; nil disables scrub admin endpoints
	PeerHealth           PeerHealthAPI             // optional; nil disables cluster peer admin endpoints
	VlogBreakdown        VlogBreakdownAPI          // optional; nil disables vlog breakdown endpoint
	ScrubProposer        ScrubProposer             // optional; nil disables POST /v1/scrub
	ScrubAggregator      ScrubAggregator           // optional; nil → GET /v1/scrub/jobs/<id> returns local-only
	IAM                  IAMService                // optional; nil disables IAM admin endpoints
	IAMPolicy            IAMPolicyService          // optional; nil disables IAM policy admin endpoints
	IAMGroup             IAMGroupService           // optional; nil disables IAM group admin endpoints
	BucketWithPolicyProp BucketWithPolicyProposer  // optional; nil → create-only path (no attach)
	ConfigProposer       ConfigProposer            // optional; nil disables config write endpoints
	ConfigStore          ConfigStoreReader         // optional; nil disables config read endpoints
	PDPTokens            PDPTokenManager           // optional; nil disables IAM PDP token endpoints
	Buckets              BucketOps                 // optional; nil disables bucket CRUD admin endpoints
	ProtocolCredentials  ProtocolCredentialService // optional; nil disables protocol credential endpoints
	ProtocolCredAuthz    CredentialAuthorizer      // required when ProtocolCredentials is configured; nil fails closed
	AdminAuthz           CredentialAuthorizer      // optional; bearer actor authz for selected admin routes
	ActorAuth            ActorAuthenticator        // optional; bearer actor auth for selected admin routes
	Status               StatusService             // optional; nil disables GET /v1/status
	Token                *dashboard.TokenStore
	PublicURL            string // e.g. "https://node1:9000"; empty means use localhost fallback
	NodeID               string
}

type ActorAuthenticator interface {
	AuthenticateActor(ctx context.Context, bearerToken string) (principal.Principal, error)
}

type ScrubJobInfo = adminapi.ScrubJobInfo
type ListScrubJobsResp = adminapi.ListScrubJobsResp
type CredentialCreateReq = adminapi.CredentialCreateReq
type CredentialListReq = adminapi.CredentialListReq
type CredentialResp = adminapi.CredentialResp
type CredentialListResp = adminapi.CredentialListResp
type CredentialRevokeResp = adminapi.CredentialRevokeResp
type BucketPolicyResp = adminapi.BucketPolicyResp
type BucketPolicySetReq = adminapi.BucketPolicySetReq
type BucketVersioningResp = adminapi.BucketVersioningResp
type BucketVersioningSetReq = adminapi.BucketVersioningSetReq
