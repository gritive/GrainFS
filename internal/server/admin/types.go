// Package admin provides transport-agnostic handlers for GrainFS administrative
// operations (volume lifecycle, dashboard token issuance). The same handler
// functions are wired into both the Unix-socket admin server (used by the
// `grainfs` CLI) and the data-plane `/ui/api/*` routes (used by the web UI).
package admin

import (
	"github.com/gritive/GrainFS/internal/adminapi"
	"github.com/gritive/GrainFS/internal/dashboard"
	"github.com/gritive/GrainFS/internal/incident"
	"github.com/gritive/GrainFS/internal/server/execution"
	"github.com/gritive/GrainFS/internal/volume"
)

// Deps bundles the shared dependencies required by every admin handler.
// Caller is responsible for constructing this struct at process startup.
type Deps struct {
	Manager              *volume.Manager
	Incident             incident.StateStore      // List(ctx, limit) — optional, nil OK
	Director             DirectorAPI              // optional; nil disables scrub admin endpoints
	PeerHealth           PeerHealthAPI            // optional; nil disables cluster peer admin endpoints
	VlogBreakdown        VlogBreakdownAPI         // optional; nil disables vlog breakdown endpoint
	ScrubProposer        ScrubProposer            // optional; nil disables POST /v1/scrub
	Execution            execution.Executor       // optional; nil uses existing ScrubProposer path
	ScrubAggregator      ScrubAggregator          // optional; nil → GET /v1/scrub/jobs/<id> returns local-only
	VolumePlacement      VolumePlacementSource    // optional; nil disables replica/EC volume health signal
	IAM                  IAMService               // optional; nil disables IAM admin endpoints
	IAMPolicy            IAMPolicyService         // optional; nil disables IAM policy admin endpoints
	IAMGroup             IAMGroupService          // optional; nil disables IAM group admin endpoints
	BucketWithPolicyProp BucketWithPolicyProposer // optional; nil → create-only path (no attach)
	ConfigProposer       ConfigProposer           // optional; nil disables config write endpoints
	ConfigStore          ConfigStoreReader        // optional; nil disables config read endpoints
	Buckets              BucketOps                // optional; nil disables bucket CRUD admin endpoints
	NfsExports           NfsExportService         // optional; nil disables NFS export admin endpoints
	IcebergConfig        IcebergConfigService     // optional; nil disables iceberg config endpoint
	AuditQuery           AuditQueryService        // optional; nil disables audit admin endpoints
	Status               StatusService            // optional; nil disables GET /v1/status
	Protocols            StorageProtocolStatusResp
	NFSDiag              NFSDiag // optional; nil disables live NFS lookup/client diagnostics
	Token                *dashboard.TokenStore
	PublicURL            string // e.g. "https://node1:9000"; empty means use localhost fallback
	NodeID               string
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
type IcebergConfigRequest = adminapi.IcebergConfigRequest
type IcebergConfigResponse = adminapi.IcebergConfigResponse
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
