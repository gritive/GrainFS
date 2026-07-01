package cluster

import "github.com/gritive/GrainFS/internal/adminapi"

// PlacementReport and PlacementReportEntry are aliases to the wire types in
// adminapi (single source of truth).
type (
	PlacementReport      = adminapi.PlacementReport
	PlacementReportEntry = adminapi.PlacementReportEntry
)
