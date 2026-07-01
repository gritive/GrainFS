// Package admincli contains the shared admin HTTP client used by the
// `grainfs` CLI commands (iam, cluster, credential, scrub, …). It provides the
// base Client/Transport plumbing plus the scrub session endpoints and the
// follow-loop reused by the EC bucket-scrub CLI.
package admincli

import (
	"github.com/gritive/GrainFS/internal/adminapi"
)

type ScrubTriggerResp = adminapi.ScrubResp
type ScrubJobInfo = adminapi.ScrubJobInfo
type ListScrubJobsResp = adminapi.ListScrubJobsResp
