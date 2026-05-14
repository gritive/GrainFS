// Package admin provides transport-agnostic handlers for GrainFS administrative
// operations (volume lifecycle, dashboard token issuance). The same handler
// functions are wired into both the Unix-socket admin server (used by the
// `grainfs` CLI) and the data-plane `/ui/api/*` routes (used by the web UI).
package admin

import (
	"context"
	"encoding/json"

	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/iam"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/scrubber"
	"github.com/gritive/GrainFS/internal/volume"
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

type ClusterPeerInfo = adminapi.ClusterPeerInfo
type ListClusterPeersResp = adminapi.ListClusterPeersResp

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

type ScrubReq = adminapi.ScrubReq
type ScrubResp = adminapi.ScrubResp

// VlogBreakdownAPI is the slim interface admin handlers need to surface the
// vlog watcher's per-category state. Implemented by an adapter in serve.go
// over *resourcewatch.Registry + VlogProvider; defined here so handler tests
// can substitute a mock.
type VlogBreakdownAPI interface {
	Breakdown() (VlogBreakdownResp, error)
}

type VlogBreakdownResp = adminapi.VlogBreakdownResp
type VlogCategoryBytes = adminapi.VlogCategoryBytes
type VlogSmokeReport = adminapi.VlogSmokeReport

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
	Upsert(ctx context.Context, bucket string, params NfsExportUpsertParams) error
	Delete(ctx context.Context, bucket string) error
	DeleteAfterBucketDelete(ctx context.Context, bucket string, force bool) error
	Get(bucket string) (NfsExportInfo, bool)
	List() []NfsExportInfo
}

type NfsExportUpsertParams struct {
	ReadOnly bool
}

type NfsExportInfo = adminapi.NfsExportInfo
type NfsExportUpsertReq = adminapi.NfsExportUpsertReq
type ListNfsExportsResp = adminapi.ListNfsExportsResp

// IAMService is the slim interface the IAM admin handlers need.
// Satisfied by *iam.AdminAPI.
type IAMService interface {
	CreateSA(ctx context.Context, req iam.SACreateRequest) (iam.SACreateResponse, error)
	ListSA(ctx context.Context) ([]iam.SAListItem, error)
	GetSA(ctx context.Context, saID string) (iam.SAGetResponse, error)
	DeleteSA(ctx context.Context, saID string) error
	CreateKey(ctx context.Context, saID string, req iam.KeyCreateRequest) (iam.KeyCreateResponse, error)
	RevokeKey(ctx context.Context, saID, accessKey string) error
	PutGrant(ctx context.Context, req iam.GrantPutRequest) error
	DeleteGrant(ctx context.Context, req iam.GrantDeleteRequest) error
	ListGrants(ctx context.Context, saFilter, bucketFilter string) ([]iam.GrantListItem, error)
	PutBucketUpstream(ctx context.Context, req iam.BucketUpstreamPutRequest) error
	GetBucketUpstream(ctx context.Context, bucket string) (iam.BucketUpstreamItem, error)
	ListBucketUpstreams(ctx context.Context) ([]iam.BucketUpstreamItem, error)
	DeleteBucketUpstream(ctx context.Context, bucket string) error
}

// Deps bundles the shared dependencies required by every admin handler.
// Caller is responsible for constructing this struct at process startup.
type Deps struct {
	Manager         *volume.Manager
	Incident        incident.StateStore   // List(ctx, limit) — optional, nil OK
	Director        DirectorAPI           // optional; nil disables scrub admin endpoints
	PeerHealth      PeerHealthAPI         // optional; nil disables cluster peer admin endpoints
	VlogBreakdown   VlogBreakdownAPI      // optional; nil disables vlog breakdown endpoint
	ScrubProposer   ScrubProposer         // optional; nil disables POST /v1/scrub
	ScrubAggregator ScrubAggregator       // optional; nil → GET /v1/scrub/jobs/<id> returns local-only
	VolumePlacement VolumePlacementSource // optional; nil disables replica/EC volume health signal
	IAM             IAMService            // optional; nil disables IAM admin endpoints
	Buckets         BucketOps             // optional; nil disables bucket CRUD admin endpoints
	NfsExports      NfsExportService      // optional; nil disables NFS export admin endpoints
	Token           *dashboard.TokenStore
	PublicURL       string // e.g. "https://node1:9000"; empty means use localhost fallback
	NodeID          string
}

type Error = adminapi.Error

func NewNotFound(msg string) *Error  { return &Error{Code: "not_found", Message: msg} }
func NewInvalid(msg string) *Error   { return &Error{Code: "invalid", Message: msg} }
func NewForbidden(msg string) *Error { return &Error{Code: "forbidden", Message: msg} }
func NewInternal(msg string) *Error  { return &Error{Code: "internal", Message: msg} }
func NewConflict(msg string, details map[string]any) *Error {
	raw, _ := json.Marshal(details)
	return &Error{Code: "conflict", Message: msg, Details: raw}
}
func NewUnsupported(msg string, details map[string]any) *Error {
	raw, _ := json.Marshal(details)
	return &Error{Code: "unsupported", Message: msg, Details: raw}
}
func NewRetry(msg string) *Error { return &Error{Code: "retry", Message: msg} }
func NewBucketNotFound(bucket string) *Error {
	return (&Error{Code: "bucket_not_found", Message: "bucket '" + bucket + "' does not exist"}).
		WithParam("bucket").
		WithHelp("Create the bucket first with 'grainfs bucket create " + bucket + "', then re-run.").
		WithDocs("https://github.com/gritive/GrainFS/docs/nfs-export-lifecycle.md#bucket-not-found")
}
func NewExportNotFound(bucket string) *Error {
	return (&Error{Code: "export_not_found", Message: "NFS export '" + bucket + "' is not registered"}).
		WithParam("bucket").
		WithHelp("List existing exports with 'grainfs nfs export list'.").
		WithDocs("https://github.com/gritive/GrainFS/docs/nfs-export-lifecycle.md#export-not-found")
}

type WriteAtVolumeReq = adminapi.WriteAtVolumeReq
type WriteAtVolumeResp = adminapi.WriteAtVolumeResp
type ReadAtVolumeReq = adminapi.ReadAtVolumeReq
type ReadAtVolumeResp = adminapi.ReadAtVolumeResp
type ScrubVolumeReq = adminapi.ScrubVolumeReq
type ScrubVolumeResp = adminapi.ScrubVolumeResp
type ScrubJobInfo = adminapi.ScrubJobInfo
type ListScrubJobsResp = adminapi.ListScrubJobsResp
type VolumeInfo = adminapi.VolumeInfo
type BucketPolicyResp = adminapi.BucketPolicyResp
type BucketPolicySetReq = adminapi.BucketPolicySetReq
type BucketVersioningResp = adminapi.BucketVersioningResp
type BucketVersioningSetReq = adminapi.BucketVersioningSetReq

func toVolumeInfo(v *volume.Volume) VolumeInfo {
	return VolumeInfo{
		Name:            v.Name,
		Size:            v.Size,
		BlockSize:       v.BlockSize,
		AllocatedBlocks: v.AllocatedBlocks,
		AllocatedBytes:  v.AllocatedBytes(),
		SnapshotCount:   v.SnapshotCount,
		Health:          "ok",
		HealthReasons:   []string{},
	}
}
