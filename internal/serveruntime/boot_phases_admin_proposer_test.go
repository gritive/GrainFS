package serveruntime

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/gritive/GrainFS/internal/iam"
)

// A nil *iam.MetaProposer boxed directly into the admin.BucketWithPolicyProposer
// interface yields a NON-nil interface (typed-nil), which defeats the != nil
// guard at handlers_bucket.go:43. The helper must collapse a nil concrete
// pointer to an untyped-nil interface so the guard holds. Mirrors the existing
// bucketUpstreamDeleteProposer guard.
func TestBucketWithPolicyProposer_NilIAMProposer_ReturnsUntypedNil(t *testing.T) {
	got := bucketWithPolicyProposer(&bootState{})
	require.Nil(t, got, "nil iamProposer must yield an untyped-nil interface, not a boxed typed-nil")
}

func TestBucketWithPolicyProposer_NonNil_ReturnsProposer(t *testing.T) {
	state := &bootState{iamProposer: &iam.MetaProposer{}}
	require.NotNil(t, bucketWithPolicyProposer(state))
}
