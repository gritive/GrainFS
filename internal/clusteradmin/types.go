package clusteradmin

import "github.com/gritive/GrainFS/internal/adminapi"

// Wire-shape aliases. The canonical definitions live in adminapi.
type (
	Status               = adminapi.Status
	ShardGroup           = adminapi.ShardGroup
	Health               = adminapi.Health
	QuorumInfo           = adminapi.QuorumInfo
	PeerHealthRow        = adminapi.PeerHealthRow
	PlacementReport      = adminapi.PlacementReport
	PlacementReportEntry = adminapi.PlacementReportEntry
	BalancerStatus       = adminapi.BalancerStatus
	BalancerNodeStatus   = adminapi.BalancerNodeStatus
	Event                = adminapi.Event
	TransferLeaderResult = adminapi.TransferLeaderResult
	PeerLivenessRow      = adminapi.PeerLivenessRow
)

// PlacementOptions is request-side input (not a wire body); stays here.
type PlacementOptions struct {
	Bucket string
	Key    string
	Limit  int
}
