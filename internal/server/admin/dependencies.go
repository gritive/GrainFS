package admin

import (
	"context"
	"time"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/nfs4server"
	"github.com/gritive/GrainFS/internal/scrubber"
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

// VolumePlacementSource is the slim interface admin handlers need to obtain
// per-volume replica/EC actual layout signals (ADR 0007) for volume health
// composition. Implemented by an adapter over the cluster meta-Raft FSM;
// defined here so handler tests can substitute a fake. nil VolumePlacement
// (or a non-cluster runtime) disables the replica contribution to volume
// health, leaving incident-only signals.
type VolumePlacementSource interface {
	VolumeReplicaSummaries(ctx context.Context, names []string) (map[string]ReplicaLayoutFact, error)
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

type NfsExportService interface {
	Create(ctx context.Context, bucket string, params NfsExportUpsertParams) error
	Upsert(ctx context.Context, bucket string, params NfsExportUpsertParams) error
	Delete(ctx context.Context, bucket string) error
	DeleteForBucketDelete(ctx context.Context, bucket string, force bool) error
	RestoreForBucketDelete(ctx context.Context, info NfsExportInfo) error
	MarkBucketDeleteCleanup(bucket string) error
	ClearBucketDeleteCleanup(bucket string) error
	Get(bucket string) (NfsExportInfo, bool)
	List() []NfsExportInfo
}

type NfsExportUpsertParams struct {
	ReadOnly bool
}

type NFSDiag interface {
	RecentLookups(bucket string, window time.Duration) []nfs4server.LookupRecord
	ActiveMountClients(bucket string) []string
}

// BucketWithPolicyProposer is the slim interface the bucket create-with-attach
// path needs to propose MetaCmd 62 (CreateBucketWithPolicyAttach, D#13).
// Satisfied by *iam.MetaProposer. nil disables the attach path — CreateBucket
// falls back to the existing create-only flow.
type BucketWithPolicyProposer interface {
	ProposeCreateBucketWithPolicyAttach(ctx context.Context, bucket, sa, policy string) error
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
type NfsExportInfo = adminapi.NfsExportInfo
type NfsExportUpsertReq = adminapi.NfsExportUpsertReq
type ListNfsExportsResp = adminapi.ListNfsExportsResp
type ExportDebugResp = adminapi.ExportDebugResp
type ExportDebugLookup = adminapi.ExportDebugLookup
type ListStorageBucketsResp = adminapi.ListStorageBucketsResp
type StorageBucketSummary = adminapi.StorageBucketSummary
type StorageBucketNFSExport = adminapi.StorageBucketNFSExport
type StorageProtocolStatusResp = adminapi.StorageProtocolStatusResp
