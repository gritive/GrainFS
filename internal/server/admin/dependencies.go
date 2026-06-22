package admin

import (
	"context"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/config"
	"github.com/gritive/GrainFS/internal/iam"
	iampdp "github.com/gritive/GrainFS/internal/iam/pdp"
	"github.com/gritive/GrainFS/internal/iam/policy"
	"github.com/gritive/GrainFS/internal/iam/principal"
	"github.com/gritive/GrainFS/internal/protocred"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/storage"
)

// DirectorAPI is the slim interface admin handlers need from the scrub
// director. Implemented by *scrubber.Director; defined here so handler
// tests can substitute a mock.
type DirectorAPI interface {
	Trigger(req scrubber.TriggerReq) (string, bool)
	Sessions() []scrubber.Session
	GetSession(id string) (scrubber.Session, bool)
	CancelSession(id string) error
	ApplyFromFSM(entry scrubber.ScrubTriggerEntry)
}

// PeerHealthAPI is the slim interface admin handlers need from the cluster
// peer-health tracker. Implemented by an adapter around *cluster.PeerHealth
// in serve.go; defined here in admin types to avoid pulling cluster into the
// admin handler tests.
type PeerHealthAPI interface {
	Snapshot() []ClusterPeerInfo
}

// ScrubProposer is the slim interface admin handlers need to publish a
// cluster-wide scrub trigger via raft. Implemented by an adapter in serve.go
// that wires MetaRaft.ProposeScrubTrigger. created=false signals a dedup hit
// (LookupDedup matched a still-tracked session); the SessionID belongs to
// the pre-existing session so polling continues to work for the original
// trigger.
type ScrubProposer interface {
	Propose(ctx context.Context, req scrubber.TriggerReq) (entry scrubber.ScrubTriggerEntry, created bool, err error)
}

// ScrubAggregator returns per-peer ScrubJobInfo (excluding local) for a given
// SessionID, plus the list of peer node IDs whose RPC failed/timed out. nil
// disables cluster-wide aggregation; GET /v1/scrub/jobs/<id> returns
// local-only stats in that case.
type ScrubAggregator interface {
	Peers(ctx context.Context, sessionID string) ([]ScrubJobInfo, []string, error)
}

// VlogBreakdownAPI is the slim interface admin handlers need to surface the
// vlog watcher's per-category state. Implemented by an adapter in serve.go
// over *resourcewatch.Registry + VlogProvider; defined here so handler tests
// can substitute a mock.
type VlogBreakdownAPI interface {
	Breakdown() (VlogBreakdownResp, error)
}

// BucketOps is the slim interface bucket admin handlers need from storage.
// Satisfied by *storage.Operations.
type BucketOps interface {
	CreateBucket(ctx context.Context, bucket string) error
	HeadBucket(ctx context.Context, bucket string) error
	DeleteBucket(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]string, error)
	ForceDeleteBucket(ctx context.Context, bucket string) error
	CountObjects(ctx context.Context, bucket string) (int64, error)
	// Policy and versioning — no ctx, matching *storage.Operations signatures.
	GetBucketPolicy(bucket string) ([]byte, error)
	SetBucketPolicy(bucket string, policyJSON []byte) error
	DeleteBucketPolicy(bucket string) error
	GetBucketVersioning(bucket string) (string, error)
	SetBucketVersioning(bucket, state string) error
}

type ProtocolCredentialService interface {
	Create(protocred.CreateRequest) (protocred.Secret, error)
	List(protocred.ListFilter) []protocred.Credential
	Get(id string) (protocred.Credential, error)
	Authenticate(protocred.AuthenticateRequest) (protocred.Credential, error)
	Rotate(id string) (protocred.Secret, error)
	Revoke(id string) error
}

type CredentialAuthorizer interface {
	Authorize(ctx context.Context, saID, bucket string, ctxReq policy.RequestContext) policy.EvalResult
	AuthorizePrincipal(ctx context.Context, p principal.Principal, bucket string, ctxReq policy.RequestContext) policy.EvalResult
}

// BucketWithPolicyProposer is the slim interface the bucket create-with-attach
// path needs to propose MetaCmd 62 (CreateBucketWithPolicyAttach, D#13).
// Satisfied by *iam.MetaProposer. nil disables the attach path — CreateBucket
// falls back to the existing create-only flow.
type BucketWithPolicyProposer interface {
	ProposeCreateBucketWithPolicyAttach(ctx context.Context, bucket, sa, policy string) error
}

// LifecycleDeleteProposer proposes a meta-Raft delete of a bucket's lifecycle
// configuration. The bucket-delete cascade uses it so a recreated same-name
// bucket does not inherit stale lifecycle config. Implemented by
// cluster.LifecycleProposer.
type LifecycleDeleteProposer interface {
	ProposeLifecycleDelete(ctx context.Context, bucket string) error
}

// BucketUpstreamDeleteProposer proposes a meta-Raft delete of a bucket's IAM
// upstream (read-through federation) record, for the same bucket-delete
// cascade. Implemented by iam.MetaProposer.
type BucketUpstreamDeleteProposer interface {
	ProposeBucketUpstreamDelete(ctx context.Context, bucket string) error
}

// IAMGroupService is the slim interface group admin handlers need.
// Kept separate from IAMPolicyService because group operations use distinct
// Raft MetaCmdTypes (52-55, 58-59) and distinct FSM stores. nil disables
// group admin endpoints.
type IAMGroupService interface {
	Propose(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

// ConfigProposer is the slim interface config admin handlers need to write
// cluster-wide config via Raft. Satisfied by *cluster.MetaRaft.
// nil disables config write endpoints.
type ConfigProposer interface {
	ProposeConfigPut(ctx context.Context, key, value string) error
	ProposeConfigDelete(ctx context.Context, key string) error
}

// PDPTokenManager is the slim accessor the PDP token admin handlers need: the
// current sealed bearer token (tri-stated: absent / ready / configured-but-error)
// and the live cluster-consistent DataEncryptor used to seal a new one. Satisfied
// by an adapter in serveruntime. nil disables the pdp token endpoints.
type PDPTokenManager interface {
	CurrentToken() (token, gen string, status iampdp.TokenStatus)
	CurrentEncryptor() storage.DataEncryptor
}

// ConfigStoreReader is the slim interface config admin handlers need to read
// the current config state. Satisfied by *config.Store.
// nil disables config read endpoints.
type ConfigStoreReader interface {
	GetString(key string) (value string, present bool)
	ListAll() []config.Entry
}

// StatusService is the slim interface the status admin handler needs.
// Satisfied by *StatusAdapter in serveruntime; nil disables the status endpoint.
type StatusService interface {
	Report() adminapi.StatusReport
}

// IcebergConfigService is the slim interface the iceberg config handler needs.
// Satisfied by an adapter in serveruntime that pulls from iam.Store.
// nil disables the iceberg config endpoint.
type IcebergConfigService interface {
	// RevealSAKeyPair returns the first active AccessKey + plaintext SecretKey
	// for the given ServiceAccount. Returns an error when the SA or any
	// active key is not found.
	RevealSAKeyPair(ctx context.Context, saID string) (accessKey, secretKey string, err error)
}

// IAMService is the slim interface the IAM admin handlers need.
// Satisfied by *iam.AdminAPI.
type IAMService interface {
	CreateSA(ctx context.Context, req iam.SACreateRequest) (iam.SACreateResponse, error)
	ListSA(ctx context.Context) ([]iam.SAListItem, error)
	GetSA(ctx context.Context, saID string) (iam.SAGetResponse, error)
	DeleteSA(ctx context.Context, saID string) error
	PutGrant(ctx context.Context, req iam.GrantPutRequest) error
	DeleteGrant(ctx context.Context, req iam.GrantDeleteRequest) error
	CreateKey(ctx context.Context, saID string, req iam.KeyCreateRequest) (iam.KeyCreateResponse, error)
	RevokeKey(ctx context.Context, saID, accessKey string) error
	PutBucketUpstream(ctx context.Context, req iam.BucketUpstreamPutRequest) error
	GetBucketUpstream(ctx context.Context, bucket string) (iam.BucketUpstreamItem, error)
	ListBucketUpstreams(ctx context.Context) ([]iam.BucketUpstreamItem, error)
	DeleteBucketUpstream(ctx context.Context, bucket string) error
	CutoverBucketUpstream(ctx context.Context, bucket string) error
}

type ClusterPeerInfo = adminapi.ClusterPeerInfo
type ListClusterPeersResp = adminapi.ListClusterPeersResp
type ScrubReq = adminapi.ScrubReq
type ScrubResp = adminapi.ScrubResp
type VlogBreakdownResp = adminapi.VlogBreakdownResp
type VlogCategoryBytes = adminapi.VlogCategoryBytes
type VlogSmokeReport = adminapi.VlogSmokeReport
type ListStorageBucketsResp = adminapi.ListStorageBucketsResp
type StorageBucketSummary = adminapi.StorageBucketSummary
