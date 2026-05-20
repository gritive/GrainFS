package serveruntime

import (
	"context"

	"github.com/gritive/GrainFS/internal/cluster/clusterpb"
	"github.com/gritive/GrainFS/internal/server/admin"
)

// iamGroupAdminAdapter bridges MetaRaft.Propose into admin.IAMGroupService.
// Group operations do not need store reads (all mutations; reads go via the
// policy resolver), so only the propose func is needed.
type iamGroupAdminAdapter struct {
	propose func(ctx context.Context, cmdType clusterpb.MetaCmdType, payload []byte) error
}

var _ admin.IAMGroupService = (*iamGroupAdminAdapter)(nil)

func (a *iamGroupAdminAdapter) Propose(
	ctx context.Context,
	cmdType clusterpb.MetaCmdType,
	payload []byte,
) error {
	return a.propose(ctx, cmdType, payload)
}
